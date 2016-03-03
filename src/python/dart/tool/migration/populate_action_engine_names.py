import logging
import traceback

from dart.context.database import db
from dart.model.orm import ActionDao, DatastoreDao, WorkflowDao
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class PopulateActionEngineNames(Tool):
    def __init__(self):
        super(PopulateActionEngineNames, self).__init__(_logger)

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
                    if not action.data.engine_name:
                        engine_name = self._get_engine_from_datastore_id(action.data.datastore_id)
                        if not engine_name:
                            engine_name = self._get_engine_from_workflow_id(action.data.workflow_id)

                        source_action = action.copy()
                        action.data.engine_name = engine_name
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

    @staticmethod
    def _get_engine_from_datastore_id(datastore_id):
        if not datastore_id:
            return None
        datastore_dao = DatastoreDao.query.get(datastore_id)
        if not datastore_dao:
            return None
        datastore = datastore_dao.to_model()
        return datastore.data.engine_name

    def _get_engine_from_workflow_id(self, workflow_id):
        if not workflow_id:
            return None
        workflow_dao = WorkflowDao.query.get(workflow_id)
        if not workflow_dao:
            return None
        workflow = workflow_dao.to_model()
        return self._get_engine_from_datastore_id(workflow.data.datastore_id)


if __name__ == '__main__':
    PopulateActionEngineNames().run()
