import jsonpatch
from retrying import retry
from sqlalchemy.orm.exc import StaleDataError
from dart.context.database import db
from dart.model.exception import DartConditionalUpdateFailedException


def patch_difference(dao, src_model, dest_model, commit=True, conditional=None):
    patch = jsonpatch.make_patch(src_model.to_dict(), dest_model.to_dict())
    return patch_data(dao, src_model.id, patch, commit, conditional)


def _retry_stale_data_error(exception):
    if isinstance(exception, StaleDataError):
        db.session.rollback()
        return True
    return False


retry_stale_data = retry(wait_random_min=1, wait_random_max=500, retry_on_exception=_retry_stale_data_error)


@retry_stale_data
def patch_data(dao, model_id, patch, commit=True, conditional=None):
    dao_instance = dao.query.get(model_id)
    model = dao_instance.to_model()
    if conditional and not conditional(model):
        raise DartConditionalUpdateFailedException('specified conditional failed')
    patched_dict = patch.apply(model.to_dict())
    for k, v in patched_dict.iteritems():
        setattr(dao_instance, k, v)
    if commit:
        db.session.commit()
    return dao_instance.to_model()
