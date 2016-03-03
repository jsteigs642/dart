from datetime import datetime, timedelta
from functools import wraps

from retrying import retry
from sqlalchemy.orm.exc import StaleDataError, NoResultFound

from dart.context.database import db
from dart.model.exception import DartLockTimeoutException
from dart.model.mutex import MutexState
from dart.model.orm import MutexDao


def db_mutex(name, timeout_sec=120):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            expiration_time = datetime.utcnow() + timedelta(seconds=timeout_sec)
            acquire_lock(name, expiration_time)
            try:
                return f(*args, **kwargs)
            finally:
                release_lock(name)
        return wrapper
    return decorator


def _retry_lock_errors(exception):
    if isinstance(exception, StaleDataError) or isinstance(exception, NoResultFound):
        db.session.rollback()
        return True
    return False


retry_until_lock_acquired = retry(wait_random_min=1000, wait_random_max=5000, retry_on_exception=_retry_lock_errors)


@retry_until_lock_acquired
def acquire_lock(name, expiration_time):
    if datetime.utcnow() > expiration_time:
        raise DartLockTimeoutException('failed to acquire lock in time: %s' % name)
    mutex_dao = MutexDao\
        .query\
        .filter(MutexDao.name == name)\
        .filter(MutexDao.state == MutexState.READY)\
        .one()
    mutex_dao.state = MutexState.LOCKED
    db.session.commit()


def release_lock(name):
    try:
        mutex_dao = MutexDao\
            .query\
            .filter(MutexDao.name == name)\
            .filter(MutexDao.state == MutexState.LOCKED)\
            .one()
        mutex_dao.state = MutexState.READY
        db.session.commit()
    except NoResultFound:
        # this means we never acquired the lock, probably due to timeout
        pass
