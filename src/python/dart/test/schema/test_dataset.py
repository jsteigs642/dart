import unittest

from dart.model.dataset import Column, DataFormat, Dataset, DatasetData, DataType, RowFormat, FileFormat
from dart.model.exception import DartValidationException
from dart.schema.base import default_and_validate
from dart.schema.dataset import dataset_schema


class TestDatasetSchema(unittest.TestCase):

    def test_dataset_schema(self):
        columns = [Column('c1', DataType.VARCHAR, 50), Column('c2', DataType.BIGINT)]
        num_header_rows = None
        df = DataFormat(FileFormat.PARQUET, RowFormat.NONE, num_header_rows)
        ds = Dataset(data=DatasetData('test-dataset', 'test_dataset_table', 's3://bucket/prefix', df, columns))
        obj_before = ds.to_dict()
        obj_after = default_and_validate(ds, dataset_schema()).to_dict()
        # num_header_rows should have been defaulted to 0, making these unequal
        self.assertNotEqual(obj_before, obj_after)

    def test_dataset_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            columns = [Column('c1', DataType.VARCHAR, 50), Column('c2', DataType.BIGINT)]
            df = DataFormat(FileFormat.PARQUET, RowFormat.NONE)
            location = None
            ds = Dataset(data=DatasetData('test-dataset', 'test_dataset_table', location, df, columns))
            # should fail because location is required
            default_and_validate(ds, dataset_schema())

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
