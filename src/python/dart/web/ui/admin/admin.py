from flask import Blueprint
from sqlalchemy import text

from dart.context.database import db
from dart.model.mutex import Mutexes, MutexState
from dart.util.rand import random_id

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/create_all', methods=['POST'])
def create_all():
    db.create_all()

    for mutex in Mutexes.all():
        sql = """
            INSERT INTO mutex (id, version_id, created, updated, name, state)
            SELECT :id, 0, NOW(), NOW(), :name, :state
            WHERE NOT EXISTS (SELECT NULL FROM mutex WHERE name = :name)
            """
        statement = text(sql).bindparams(id=random_id(), name=mutex, state=MutexState.READY)
        db.session.execute(statement)
        db.session.commit()

    return 'OK'
