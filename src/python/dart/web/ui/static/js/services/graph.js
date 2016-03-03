angular
    .module('dart.services.graph', [])
    .factory('GraphService', ['$resource', 'dtUtils', function($resource, Utils) {
        function dataApi() { return $resource('api/1/graph/:type/:id', null) }
        function identifierApi() { return $resource('api/1/graph/entity_identifiers', null) }
        function subGraphApi() { return $resource('api/1/graph/sub_graph', null) }
        return {
            getGraphIdentifiers: function(search) { return Utils.wrap(identifierApi().get({search: search}).$promise) },
            getGraphData: function(type, id) { return Utils.wrap(dataApi().get({type: type, id: id}).$promise) },
            getSubGraphMap: function(related_type, related_id, engine_name) {
                return Utils.wrap(subGraphApi().get({related_type: related_type, related_id: related_id, engine_name: engine_name}).$promise)
            },
            saveEntityMap: function(entity_map, debug) { return Utils.wrap(subGraphApi().save({debug: debug}, entity_map).$promise, true) }
        };
    }])
;
