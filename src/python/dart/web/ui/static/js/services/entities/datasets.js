angular
    .module('dart.services.entities')
    .factory('DatasetService', ['$resource', 'dtUtils', function($resource, Utils) {
        function api() { return $resource('api/1/dataset/:id', null, { 'update': { method: 'PUT' }}) }
        return {
            getSchema: function() { return Utils.wrap($resource('api/1/schema/dataset').get().$promise) },
            getEntity: function(id) { return Utils.wrap(api().get({id: id}).$promise) },
            getEntities: function(limit, offset, filters) { return Utils.wrap(api().get({limit: limit, offset: offset, filters: JSON.stringify(filters)}).$promise) },
            saveEntity: function(e) { return Utils.wrap(api().save(null, Utils.stripSingleArrayElementNulls(e)).$promise, true) },
            updateEntity: function(e) { return Utils.wrap(api().update({id: e.id }, Utils.stripSingleArrayElementNulls(e)).$promise, true) }
        };
    }])
;