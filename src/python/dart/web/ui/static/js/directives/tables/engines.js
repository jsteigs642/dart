angular
    .module('dart.directives.tables')
    .directive('dtEnginesTable', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/tables/engines.html',
            controller: ['$scope', 'EngineService', 'EntityModalService',
                function($scope, EngineService, EntityModalService) {
                    $scope.query = { limit: 10, page: 1 };
                    $scope.onPaginationChange = function(page, limit) {
                        if ($scope.options.onTableChange) {
                            $scope.options.onTableChange(page, limit, $scope.filters)
                        }
                        $scope.$p = EngineService.getEntities(limit, (page - 1) * limit, $scope.filters);
                        $scope.$p.then(function(response) { $scope.response = response });
                        return $scope.$p;
                    };
                    $scope.onPaginationChange(1, $scope.query.limit);
                    $scope.showEntityModal = function(ev, entity, mode) {
                        EntityModalService.showDialog(ev, entity,
                            function () {
                                return EngineService.getSchema().then(function(response) {
                                    if (mode === 'view') {
                                        response.results.readonly = true;
                                    }
                                    return response
                                })
                            }
                        )
                    };
                }
            ]
        }
    }])
;