angular
    .module('dart.services.entities')
    .factory('ActionService', ['$resource', 'dtUtils', function($resource, Utils) {
        return {
            getSchema: function(options) {
                var args;
                if (!options) {
                    args = {}
                } else if (options.entity_id) {
                    args = {
                        action_id: options.entity_id
                    }
                } else if (options.entity) {
                    args = {
                        datastore_id: options.entity.data.datastore_id,
                        workflow_id: options.entity.data.workflow_id,
                        action_type_name: options.entity.data.action_type_name
                    }
                } else {
                    args = {
                        engine_name: options.engine_name,
                        action_type_name: options.action_type_name
                    }
                }
                return Utils.wrap($resource('api/1/schema/action/:action_type_name').get(args).$promise)
            },
            getEntity: function(id) { return Utils.wrap($resource('api/1/action/:id').get({id: id}).$promise) },
            getEntities: function(limit, offset, filters, order_by) {
                var args = {limit: limit, offset: offset, filters: JSON.stringify(filters), order_by: JSON.stringify(order_by)};
                return Utils.wrap($resource('api/1/action').get(args).$promise)
            },
            saveEntity: function(e) {
                var args = e.data.workflow_id ?
                {action_type: 'workflow', action_type_id: e.data.workflow_id} :
                {action_type: 'datastore', action_type_id: e.data.datastore_id};
                return Utils.wrap($resource('api/1/:action_type/:action_type_id/action')
                    .save(args, Utils.stripSingleArrayElementNulls(e)).$promise, true);
            },
            updateEntity: function(e) {
                var api = $resource('api/1/action/:id', null, { 'update': { method: 'PUT' }});
                return Utils.wrap(api.update({id: e.id }, Utils.stripSingleArrayElementNulls(e)).$promise, true)
            },
            deleteEntity: function(id) {
                return Utils.wrap($resource('api/1/action/:id').delete({id: id}).$promise, true);
            }
        };
    }])
;