import logging
import traceback

from dart.context.database import db
from dart.model.orm import ActionDao
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class MigrateConsumeSubscriptionActions(Tool):
    def __init__(self):
        super(MigrateConsumeSubscriptionActions, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE action DISABLE TRIGGER action_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))

                action_daos = ActionDao.query\
                    .filter(ActionDao.data['action_type_name'].astext == 'consume_subscription_assignment')\
                    .order_by(ActionDao.created)\
                    .limit(limit)\
                    .offset(offset)\
                    .all()

                if len(action_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for action_dao in action_daos:
                    action = action_dao.to_model()
                    source_action = action.copy()
                    action.data.action_type_name = 'consume_subscription'
                    patch_difference(ActionDao, source_action, action, commit=False)

                db.session.commit()
                _logger.info('completed batch with limit=%s offset=%s' % (limit, offset))
                offset += limit

        except Exception as e:
            db.session.rollback()
            _logger.error(traceback.format_exc())
            raise e

        finally:
            db.session.execute('ALTER TABLE action ENABLE TRIGGER action_update_timestamp')


if __name__ == '__main__':
    MigrateConsumeSubscriptionActions().run()
