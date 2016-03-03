angular
    .module('dart.directives.tables')
    .directive('dtWorkflowInstancesTable', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/tables/workflow_instances.html',
            controller: ['$scope', 'WorkflowInstanceService', 'EntityModalService', 'FilterService',
                function($scope, WorkflowInstanceService, EntityModalService, FilterService) {
                    $scope.filters = $scope.options.filters || [];
                    $scope.hiddenFilters = $scope.options.hiddenFilters || [];
                    $scope.$watchCollection("filters", function (newCollection, oldCollection) {
                            if (newCollection.length === 0 && oldCollection.length === 0) { return }
                            $scope.onPaginationChange(1, $scope.query.limit);
                        }
                    );
                    $scope.query = { limit: 10, page: 1 };
                    $scope.onPaginationChange = function(page, limit) {
                        if ($scope.options.onTableChange) {
                            $scope.options.onTableChange(page, limit, $scope.filters)
                        }
                        $scope.$p = WorkflowInstanceService.getEntities(limit, (page - 1) * limit, $scope.filters.concat($scope.hiddenFilters));
                        $scope.$p.then(function(response) { $scope.response = response });
                        return $scope.$p;
                    };
                    $scope.onPaginationChange(1, $scope.query.limit);
                    $scope.showEntityModal = function(ev, entity, mode) {
                        EntityModalService.showDialog(ev, entity,
                            function () {
                                return WorkflowInstanceService.getSchema().then(function(response) {
                                    if (mode === 'view') {
                                        response.results.readonly = true;
                                    }
                                    return response
                                })
                            }
                        )
                    };
                    WorkflowInstanceService.getSchema().then(function(response) {
                        $scope.getFilterChoices = function(searchText) { return FilterService.getFilterChoices(searchText, [response.results]) };
                    });
                }
            ]
        }
    }])
;