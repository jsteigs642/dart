angular
    .module('dart.services.entities')
    .factory('TriggerService', ['$resource', 'dtUtils', function($resource, Utils) {
        function api() { return $resource('api/1/trigger/:id', null, { 'update': { method: 'PUT' }}) }
        return {
            getSchema: function(options) {
                var args = {};
                if (options && options.entity_id) {
                    args = {trigger_id: options.entity_id}

                } else if (options && options.trigger_type_name) {
                    args = {trigger_type_name: options.trigger_type_name}
                }
                return Utils.wrap($resource('api/1/schema/trigger').get(args).$promise)
            },
            getEntity: function(id) { return Utils.wrap(api().get({id: id}).$promise) },
            getEntities: function(limit, offset, filters) { return Utils.wrap(api().get({limit: limit, offset: offset, filters: JSON.stringify(filters)}).$promise) },
            saveEntity: function(e) { return Utils.wrap(api().save({id: e.id }, Utils.stripSingleArrayElementNulls(e)).$promise, true) },
            updateEntity: function(e) { return Utils.wrap(api().update({id: e.id }, Utils.stripSingleArrayElementNulls(e)).$promise, true) }
        };
    }])
;
