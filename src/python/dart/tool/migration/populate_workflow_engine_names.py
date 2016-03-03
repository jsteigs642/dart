import logging
import traceback

from dart.context.database import db
from dart.model.orm import DatastoreDao, WorkflowDao
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class PopulateWorkflowEngineNames(Tool):
    def __init__(self):
        super(PopulateWorkflowEngineNames, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE workflow DISABLE TRIGGER workflow_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))
                workflow_daos = WorkflowDao.query.order_by(WorkflowDao.created).limit(limit).offset(offset).all()
                if len(workflow_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for workflow_dao in workflow_daos:
                    workflow = workflow_dao.to_model()
                    if not workflow.data.engine_name:
                        engine_name = self._get_engine_from_datastore_id(workflow.data.datastore_id)
                        source_workflow = workflow.copy()
                        workflow.data.engine_name = engine_name
                        patch_difference(WorkflowDao, source_workflow, workflow, commit=False)

                db.session.commit()
                _logger.info('completed batch with limit=%s offset=%s' % (limit, offset))
                offset += limit

        except Exception as e:
            db.session.rollback()
            _logger.error(traceback.format_exc())
            raise e

        finally:
            db.session.execute('ALTER TABLE workflow ENABLE TRIGGER workflow_update_timestamp')

    @staticmethod
    def _get_engine_from_datastore_id(datastore_id):
        if not datastore_id:
            return None
        datastore_dao = DatastoreDao.query.get(datastore_id)
        if not datastore_dao:
            return None
        datastore = datastore_dao.to_model()
        return datastore.data.engine_name


if __name__ == '__main__':
    PopulateWorkflowEngineNames().run()
