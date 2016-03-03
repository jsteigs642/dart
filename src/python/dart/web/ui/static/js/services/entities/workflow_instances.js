angular
    .module('dart.services.entities')
    .factory('WorkflowInstanceService', ['$resource', 'dtUtils', function($resource, Utils) {
        return {
            getSchema: function() { return Utils.wrap($resource('api/1/schema/workflow/instance').get().$promise) },
            getEntity: function(id) { return Utils.wrap($resource('api/1/workflow/instance/:id').get({id: id}).$promise) },
            getEntities: function(limit, offset, filters) {
                return Utils.wrap($resource('api/1/workflow/instance').get({limit: limit, offset: offset, filters: JSON.stringify(filters)}).$promise)
            }
        }
    }])
;