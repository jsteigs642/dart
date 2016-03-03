angular
    .module('dart.services.entities')
    .factory('DatastoreService', ['$resource', 'dtUtils', function($resource, Utils) {
        function api() { return $resource('api/1/datastore/:id', null, { 'update': { method: 'PUT' }}) }
        return {
            getSchema: function(options) {
                if (!options) {
                    return Utils.wrap($resource('api/1/schema/datastore').get().$promise)
                }
                if (options.entity_id) {
                    return Utils.wrap($resource('api/1/schema/datastore').get({datastore_id: options.entity_id}).$promise)
                }
                return Utils.wrap($resource('api/1/schema/engine/:engine_name/datastore').get({ engine_name: options.engineName }).$promise)
            },
            getEntity: function(id) { return Utils.wrap(api().get({id: id}).$promise) },
            getEntities: function(limit, offset, filters) { return Utils.wrap(api().get({limit: limit, offset: offset, filters: JSON.stringify(filters)}).$promise) },
            saveEntity: function(e) { return Utils.wrap(api().save(null, Utils.stripSingleArrayElementNulls(e)).$promise, true) },
            updateEntity: function(e) { return Utils.wrap(api().update({id: e.id }, e).$promise, true) }
        };
    }])
;