import logging
import traceback

from dart.context.database import db
from dart.model.orm import WorkflowDao, WorkflowInstanceDao
from dart.service.patcher import patch_difference
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class PopulateWorkflowInstanceEngineNames(Tool):
    def __init__(self):
        super(PopulateWorkflowInstanceEngineNames, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE workflow_instance DISABLE TRIGGER workflow_instance_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))
                workflow_instance_daos = WorkflowInstanceDao.query.order_by(WorkflowInstanceDao.created).limit(limit).offset(offset).all()
                if len(workflow_instance_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for workflow_instance_dao in workflow_instance_daos:
                    workflow_instance = workflow_instance_dao.to_model()
                    if not workflow_instance.data.engine_name:
                        engine_name = self._get_engine_from_workflow_id(workflow_instance.data.workflow_id)
                        source_workflow_instance = workflow_instance.copy()
                        workflow_instance.data.engine_name = engine_name
                        patch_difference(WorkflowInstanceDao, source_workflow_instance, workflow_instance, commit=False)

                db.session.commit()
                _logger.info('completed batch with limit=%s offset=%s' % (limit, offset))
                offset += limit

        except Exception as e:
            db.session.rollback()
            _logger.error(traceback.format_exc())
            raise e

        finally:
            db.session.execute('ALTER TABLE workflow_instance ENABLE TRIGGER workflow_instance_update_timestamp')

    @staticmethod
    def _get_engine_from_workflow_id(workflow_id):
        if not workflow_id:
            return None
        workflow_dao = WorkflowDao.query.get(workflow_id)
        if not workflow_dao:
            return None
        workflow = workflow_dao.to_model()
        return workflow.data.engine_name


if __name__ == '__main__':
    PopulateWorkflowInstanceEngineNames().run()
