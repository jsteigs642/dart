import logging

from dart.context.database import db
from dart.model.orm import ActionDao
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class MigrateActionNames(Tool):
    def __init__(self):
        super(MigrateActionNames, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE action DISABLE TRIGGER action_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))
                action_daos = ActionDao.query.order_by(ActionDao.created).limit(limit).offset(offset).all()
                if len(action_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for action_dao in action_daos:
                    action = action_dao.to_model()
                    if not action.data.action_type_name:
                        source_action = action.copy()
                        action.data.action_type_name = action.data.name
                        patch_difference(ActionDao, source_action, action, commit=False)

                db.session.commit()
                _logger.info('completed batch with limit=%s offset=%s' % (limit, offset))
                offset += limit

        except Exception as e:
            db.session.rollback()
            raise e

        finally:
            db.session.execute('ALTER TABLE action ENABLE TRIGGER action_update_timestamp')


if __name__ == '__main__':
    MigrateActionNames().run()
