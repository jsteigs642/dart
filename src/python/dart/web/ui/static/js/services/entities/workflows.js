angular
    .module('dart.services.entities')
    .factory('WorkflowService', ['$resource', 'dtUtils', function($resource, Utils) {
        function api() { return $resource('api/1/workflow/:id', null, { 'update': { method: 'PUT' }}) }
        function runWorkflowApi() { return $resource('api/1/workflow/:id/do-manual-trigger', null) }
        return {
            getSchema: function() { return Utils.wrap($resource('api/1/schema/workflow').get().$promise) },
            getEntity: function(id) { return Utils.wrap(api().get({id: id}).$promise) },
            getEntities: function(limit, offset, filters) { return Utils.wrap(api().get({limit: limit, offset: offset, filters: JSON.stringify(filters)}).$promise) },
            saveEntity: function(e) {
                return Utils.wrap($resource('api/1/datastore/:datastore_id/workflow')
                    .save({datastore_id: e.data.datastore_id}, Utils.stripSingleArrayElementNulls(e)).$promise, true)
            },
            updateEntity: function(e) { return Utils.wrap(api().update({id: e.id }, e).$promise, true) },
            manuallyRunWorkflow: function(id) { return Utils.wrap(runWorkflowApi().save({id: id }, null).$promise, true) }
        };
    }])
;