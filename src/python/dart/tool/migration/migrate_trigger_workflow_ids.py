import logging

from dart.context.database import db
from dart.model.orm import TriggerDao
from dart.model.trigger import Trigger
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class MigrateTriggerWorkflowIds(Tool):
    def __init__(self):
        super(MigrateTriggerWorkflowIds, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE trigger DISABLE TRIGGER trigger_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))
                trigger_daos = TriggerDao.query.order_by(TriggerDao.created).limit(limit).offset(offset).all()
                if len(trigger_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for trigger_dao in trigger_daos:
                    workflow_id = trigger_dao.data.get('workflow_id')
                    if workflow_id:
                        trigger = trigger_dao.to_model()
                        assert isinstance(trigger, Trigger)
                        source_trigger = trigger.copy()
                        trigger.data.workflow_ids = [workflow_id]
                        patch_difference(TriggerDao, source_trigger, trigger, commit=False)

                db.session.commit()
                _logger.info('completed batch with limit=%s offset=%s' % (limit, offset))
                offset += limit

        except Exception as e:
            db.session.rollback()
            raise e

        finally:
            db.session.execute('ALTER TABLE trigger ENABLE TRIGGER trigger_update_timestamp')


if __name__ == '__main__':
    MigrateTriggerWorkflowIds().run()
