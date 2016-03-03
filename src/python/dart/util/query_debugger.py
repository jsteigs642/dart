sql = """
SELECT action.id AS action_id, action.version_id AS action_version_id, action.created AS action_created, action.updated AS action_updated, action.data AS action_data
FROM action
WHERE (action.data ->> %(data_1)s) = %(param_1)s AND (action.data ->> %(data_2)s) = %(param_2)s ORDER BY CAST(action.data ->> %(data_3)s AS FLOAT)
 LIMIT %(param_3)s


 SELECT action.id AS action_id, action.version_id AS action_version_id, action.created AS action_created, action.updated AS action_updated, action.data AS action_data
FROM action
WHERE (action.data ->> 'state') = 'HAS_NEVER_RUN'
;
AND (action.data ->> 'datastore_id') = 'UGYPDHPZBP'
 ORDER BY CAST(action.data ->> 'order_idx' AS FLOAT)
 LIMIT 1
;

 """
params = {'param_1': 'UGYPDHPZBP', 'param_3': 1, 'param_2': 'HAS_NEVER_RUN', 'data_3': 'order_idx', 'data_2': 'state', 'data_1': 'datastore_id'}

print sql % params
