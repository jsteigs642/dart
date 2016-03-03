angular
    .module('dart.services.entities')
    .factory('EngineService', ['$resource', 'dtUtils' , '$window', function($resource, Utils, $window) {
        function api() { return $resource('api/1/engine') }
        function getEntities(limit, offset) { return Utils.wrap(api().get({limit: limit, offset: offset}).$promise) }
        return {
            getSchema: function() { return Utils.wrap($resource('api/1/schema/engine').get().$promise) },
            getEntities: getEntities,
            getEnginesAndActionTypes: function(engineNameFilter) {
                return getEntities(100, 0).then(function(response) {
                    var result = {};
                    result.engines = response.results;
                    result.engineAndActionTypes = [];
                    $window._.each(result.engines, function(engine) {
                        $window._.each(engine.data.supported_action_types, function(actionType) {
                            if (!engineNameFilter || engineNameFilter === engine.data.name) {
                                result.engineAndActionTypes.push({
                                    label: engine.data.name + ': ' + actionType.name,
                                    engineAndActionType: {engine: engine, actionType: actionType}
                                })
                            }
                        })
                    });
                    return result;
                });
            }
        }
    }])
;