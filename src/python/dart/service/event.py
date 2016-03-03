from sqlalchemy import desc
from dart.context.locator import injectable
from dart.model.orm import EventDao
from dart.context.database import db
from dart.schema.base import default_and_validate
from dart.schema.event import event_schema
from dart.service.patcher import patch_difference, retry_stale_data
from dart.util.rand import random_id


@injectable
class EventService(object):
    def __init__(self, filter_service):
        self._filter_service = filter_service

    @staticmethod
    def save_event(event, commit=True, flush=False):
        """ :type event: dart.model.event.Event """
        event = default_and_validate(event, event_schema())

        event_dao = EventDao()
        event_dao.id = random_id()
        event_dao.data = event.data.to_dict()
        db.session.add(event_dao)
        if flush:
            db.session.flush()
        if commit:
            db.session.commit()
        event = event_dao.to_model()
        return event

    @staticmethod
    def get_event(event_id, raise_when_missing=True):
        event_dao = EventDao.query.get(event_id)
        if not event_dao and raise_when_missing:
            raise Exception('event with id=%s not found' % event_id)
        return event_dao.to_model() if event_dao else None

    def find_events(self, limit=20, offset=0):
        query = self.find_event_query()
        query = query.limit(limit).offset(offset)
        return [dao.to_model() for dao in query.all()]

    def find_events_count(self):
        return self.find_event_query().count()

    @staticmethod
    def find_event_query():
        return EventDao.query.order_by(EventDao.data['name'])

    def query_events(self, filters, limit=20, offset=0):
        """ :type filters: list[dart.model.query.Filter] """
        query = self._query_event_query(filters)
        query = query.limit(limit).offset(offset)
        return [e.to_model() for e in query.all()]

    def query_events_count(self, filters):
        """ :type filters: list[dart.model.query.Filter] """
        query = self._query_event_query(filters)
        return query.count()

    def _query_event_query(self, filters):
        query = EventDao.query.order_by(desc(EventDao.updated))
        for f in filters:
            query = self._filter_service.apply_filter(f, query, EventDao, [event_schema()])
        return query

    @staticmethod
    def update_event(event, name, description, state):
        """ :type event: dart.model.event.Event """
        source_event = event.copy()
        event = default_and_validate(event, event_schema())
        event.data.name = name
        event.data.description = description
        event.data.state = state
        return patch_difference(EventDao, source_event, event)

    @staticmethod
    @retry_stale_data
    def delete_event(event_id):
        event_dao = EventDao.query.get(event_id)
        db.session.delete(event_dao)
        db.session.commit()
