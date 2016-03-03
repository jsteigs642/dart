angular
    .module('dart.controllers.tools')
    .controller('GraphController', ['$scope', 'GraphService', '$q', '$stateParams', '$location', '$window', '$timeout',
        function($scope, GraphService, $q, $stateParams, $location, $window, $timeout) {

            $scope.queryGraph = function() {
                if (!$scope.selectedItem) {
                    return $q.when(null);
                }
                return GraphService.getGraphData($scope.selectedItem.entity_type, $scope.selectedItem.entity_id)
            };

            $scope.layout = 'dagre';

            $scope.initScope = function(selectedItem) {
                $scope.errorMessage = null;
                $scope.selectedItem = selectedItem;
                if (!$scope.selectedItem) {
                    $location.search('t', null);
                    $location.search('id', null);
                    return
                }
                $location.search('t', $scope.selectedItem.entity_type);
                $location.search('id', $scope.selectedItem.entity_id);

                $scope.graphOptions = null;
                $scope.queryGraph().then(function(response) {
                    if (!response) { return }
                    $window._.each(response.results.nodes, function(n) {
                        if (n.entity_type === selectedItem.entity_type && n.entity_id === selectedItem.entity_id) {
                            $scope.selectedItem = {
                                entity_type: n.entity_type,
                                entity_id: n.entity_id,
                                name: n.name
                            };
                            $scope.current_node = {
                                entity_type: n.entity_type,
                                entity_id: n.entity_id,
                                name: n.name,
                                state: n.state,
                                sub_type: n.sub_type
                            };
                            $scope.$root.$broadcast('dt-graph-node-selected', $scope.current_node);
                            $scope.graphOptions = {
                                nodes: response.results.nodes,
                                edges: response.results.edges,
                                selected_type: n.entity_type,
                                selected_id: n.entity_id,
                                layout: $scope.layout
                            };
                        }
                    });
                });
            };

            $scope.graphMenuBarOptions = {
                onSelectedItemChange: $scope.initScope,
                queryGraph: $scope.queryGraph,
                layout: $scope.layout
            };

            if ($stateParams.id) {
                $scope.initScope({entity_type: $stateParams.t, entity_id: $stateParams.id});
            }

            $scope.$on('dt-graph-node-selected', function(evt, current_node) {
                $timeout(function () {
                    $scope.current_node = current_node;
                });
            });
            $scope.$on('dt-graph-node-unselected', function(evt) {
                $timeout(function () {
                    $scope.current_node = {};
                });
            });
            $scope.$on('dt-graph-save-failed', function(evt, errorMessage) {
                $timeout(function () {
                    $scope.errorMessage = errorMessage;
                });
            });
            $scope.$on('dt-graph-reload', function(evt) {
                $timeout(function () {
                    $scope.initScope({entity_type: $stateParams.t, entity_id: $stateParams.id});
                });
            });
            $scope.$on('dt-graph-cancel-sub-graph', function(evt) {
                $timeout(function () {
                    $scope.errorMessage = null;
                });
            });
        }
    ])
;
