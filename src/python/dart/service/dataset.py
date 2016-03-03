from sqlalchemy.exc import IntegrityError as SqlAlchemyIntegrityError
from psycopg2._psycopg import IntegrityError as PostgresIntegrityError
from dart.context.locator import injectable
from dart.model.exception import DartValidationException
from dart.model.orm import DatasetDao
from dart.context.database import db
from dart.schema.base import default_and_validate
from dart.schema.dataset import dataset_schema
from dart.service.patcher import retry_stale_data
from dart.util.rand import random_id


@injectable
class DatasetService(object):
    def __init__(self, filter_service):
        self._filter_service = filter_service

    @staticmethod
    def save_dataset(dataset, commit=True, flush=False):
        """ :type dataset: dart.model.dataset.Dataset """
        dataset = default_and_validate(dataset, dataset_schema())

        dataset_dao = DatasetDao()
        dataset_dao.id = random_id()
        dataset_dao.name = dataset.data.name
        dataset.data.location = dataset.data.location.rstrip('/')
        dataset_dao.data = dataset.data.to_dict()
        db.session.add(dataset_dao)
        try:
            if flush:
                db.session.flush()
            if commit:
                db.session.commit()
            dataset = dataset_dao.to_model()
            return dataset
        except SqlAlchemyIntegrityError as e:
            if hasattr(e, 'orig') and isinstance(e.orig, PostgresIntegrityError) and e.orig.pgcode == '23505':
                raise DartValidationException('name already exists: %s' % dataset.data.name)
            raise e

    @staticmethod
    def get_dataset(dataset_id, raise_when_missing=True):
        dataset_dao = DatasetDao.query.get(dataset_id)
        if not dataset_dao and raise_when_missing:
            raise Exception('dataset with id=%s not found' % dataset_id)
        return dataset_dao.to_model() if dataset_dao else None

    def find_datasets(self, limit=20, offset=0):
        query = self._find_dataset_query()
        query = query.limit(limit).offset(offset)
        return [dao.to_model() for dao in query.all()]

    def find_datasets_count(self):
        return self._find_dataset_query().count()

    @staticmethod
    def _find_dataset_query():
        return DatasetDao.query.order_by(DatasetDao.name)

    @staticmethod
    def find_datasets_indexed(ids):
        datasets = [d.to_model() for d in DatasetDao.query.filter(DatasetDao.id.in_(ids)).all()]
        return {d.id: d for d in datasets}

    def query_datasets(self, filters, limit=20, offset=0):
        """ :type filters: list[dart.model.query.Filter] """
        query = self._query_dataset_query(filters)
        query = query.limit(limit).offset(offset)
        return [d.to_model() for d in query.all()]

    def query_datasets_count(self, filters):
        """ :type filters: list[dart.model.query.Filter] """
        query = self._query_dataset_query(filters)
        return query.count()

    def _query_dataset_query(self, filters):
        query = DatasetDao.query.order_by(DatasetDao.updated)
        for f in filters:
            query = self._filter_service.apply_filter(f, query, DatasetDao, [dataset_schema()])
        return query

    def update_dataset(self, dataset_id, dataset):
        dataset = default_and_validate(dataset, dataset_schema())
        return self.update_dataset_data(dataset_id, dataset.data)

    @staticmethod
    @retry_stale_data
    def update_dataset_data(dataset_id, dataset_data):
        dataset_dao = DatasetDao.query.get(dataset_id)
        dataset_dao.name = dataset_data.name
        dataset_data.location = dataset_data.location.rstrip('/')
        dataset_dao.data = dataset_data.to_dict()
        db.session.commit()
        return dataset_dao.to_model()

    @staticmethod
    @retry_stale_data
    def delete_dataset(dataset_id):
        dataset_dao = DatasetDao.query.get(dataset_id)
        db.session.delete(dataset_dao)
        db.session.commit()
