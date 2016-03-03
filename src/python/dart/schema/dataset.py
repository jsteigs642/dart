from dart.model.dataset import FileFormat, RowFormat, Compression, DataType, LoadType
from dart.schema.base import base_schema, tag_list_schema


def columns_schema(min_items, allow_null):
    cols_type = ['array', 'null'] if allow_null else 'array'
    return {
        'x-schema-form': {'type': 'tabarray', 'title': "{{ value.name || 'column ' + $index }}"},
        'type': cols_type,
        'items': {
            'type': 'object',
            'properties': {
                'name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'maxLength': 255},
                'data_type': {'type': 'string', 'enum': DataType.all()},
                'length': {'type': ['integer', 'null'], 'minimum': 1},
                'precision': {'type': ['integer', 'null']},
                'scale': {'type': ['integer', 'null']},
                'path': {'type': ['string', 'null']},
                'date_pattern': {'type': ['string', 'null']},
                'description': {'type': ['string', 'null']},
                'is_nullable': {'type': ['boolean', 'null'], 'default': True},
            },
            'additionalProperties': False,
            'required': ['name', 'data_type']
        },
        'minItems': min_items
    }


def dataset_schema():
    return base_schema({
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_-]+$', 'maxLength': 50},
            'table_name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'minLength': 1},
            'location': {'type': 'string', 'placeholder': 's3://', 'pattern': '^s3://.+$'},
            'load_type': {'type': 'string', 'enum': LoadType.all()},
            'data_format': {
                'type': 'object',
                'properties': {
                    'file_format': {'type': 'string', 'enum': FileFormat.all()},
                    'row_format': {'type': 'string', 'enum': RowFormat.all()},
                    'num_header_rows': {'type': 'integer', 'default': 0, 'minimum': 0},
                    'delimited_by': {'type': ['string', 'null'], 'default': None},
                    'quoted_by': {'type': ['string', 'null'], 'default': None},
                    'escaped_by': {'type': ['string', 'null'], 'default': None},
                    'null_string': {'type': ['string', 'null'], 'default': None},
                    'regex_input': {'type': ['string', 'null'], 'default': None},
                    'regex_output': {'type': ['string', 'null'], 'default': None},
                },
                'additionalProperties': False,
                'required': ['file_format', 'row_format']
            },
            'columns': columns_schema(1, False),
            'primary_keys': {
                'type': 'array', 'default': [],
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'primary_key ' + $index }}"},
                'items': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'maxLength': 127}
            },
            'merge_keys': {
                'type': 'array', 'default': [],
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'merge_key ' + $index }}"},
                'items': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'maxLength': 127}
            },
            'sort_keys': {
                'type': 'array', 'default': [],
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'sort_key ' + $index }}"},
                'items': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'maxLength': 127}
            },
            'distribution_keys': {
                'type': 'array', 'default': [],
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'distribution_key ' + $index }}"},
                'items': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'maxLength': 127}
            },
            'batch_merge_sort_keys': {
                'type': 'array', 'default': [],
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'upsert_sort_key ' + $index }}"},
                'items': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+ (ASC|DESC)$', 'maxLength': 127}
            },
            'tags': tag_list_schema(),
            'compression': {'type': 'string', 'enum': Compression.all()},
            'partitions': columns_schema(0, True),
            'hive_compatible_partition_folders': {'type': ['boolean', 'null'], 'default': False},
            'description': {'type': ['string', 'null'], 'default': None},
        },
        'additionalProperties': False,
        'required': ['name', 'table_name', 'location', 'load_type', 'data_format', 'columns', 'compression']
    })
