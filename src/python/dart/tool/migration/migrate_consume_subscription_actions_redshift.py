import copy
import json
import logging
import traceback
from sqlalchemy import text

from dart.context.database import db
from dart.model.orm import ActionDao
from dart.tool.tool_runner import Tool

_logger = logging.getLogger(__name__)


class MigrateConsumeSubscriptionActionsRedshift(Tool):
    def __init__(self):
        super(MigrateConsumeSubscriptionActionsRedshift, self).__init__(_logger)

    def run(self):
        db.session.execute('ALTER TABLE action DISABLE TRIGGER action_update_timestamp')
        try:
            limit = 100
            offset = 0
            while True:
                _logger.info('starting batch with limit=%s offset=%s' % (limit, offset))

                action_daos = ActionDao.query\
                    .filter(ActionDao.data['action_type_name'].astext == 'consume_subscription')\
                    .filter(ActionDao.data['engine_name'].astext == 'redshift_engine')\
                    .order_by(ActionDao.created)\
                    .limit(limit)\
                    .offset(offset)\
                    .all()

                if len(action_daos) == 0:
                    _logger.info('done - no more entities left')
                    break

                for action_dao in action_daos:
                    data = action_dao.data
                    args = data['args']
                    args.pop('s3_path_start_prefix_inclusive', None)
                    args.pop('s3_path_end_prefix_exclusive', None)
                    args.pop('s3_path_regex_filter', None)

                    # we need to use a manual update statement here because the sqlalchemy orm layer doesn't seem
                    # to be able to commit the removal of a key with a null value (within a JSONB column) properly
                    sql = """ UPDATE action SET data=CAST(:data AS JSONB) WHERE id=:id """
                    statement = text(sql).bindparams(id=action_dao.id, data=json.dumps(data))
                    db.session.execute(statement)

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
    MigrateConsumeSubscriptionActionsRedshift().run()
