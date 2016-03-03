angular
    .module('dart.services.entities')
    .factory('TriggerTypeService', ['$resource', 'dtUtils', function($resource, Utils) {
        return {
            getSchema: function() { return Utils.wrap($resource('api/1/schema/trigger_type').get().$promise) },
            getEntities: function(limit, offset) {
                return Utils.wrap($resource('api/1/trigger_type').get({limit: limit, offset: offset}).$promise)
            }
        };
    }])
;
