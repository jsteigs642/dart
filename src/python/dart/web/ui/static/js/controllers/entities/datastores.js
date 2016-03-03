angular
    .module('dart.controllers.entities')
    .controller('DatastoresEntityController', ['$scope', '$stateParams', '$location', 'EngineService',
        function($scope, $stateParams, $location, EngineService) {
            EngineService.getEntities(100, 0).then(function(response) {
                $scope.tableOptions = {
                    engines: response.results,
                    filters: JSON.parse($stateParams.f || '[]'),
                    onTableChange: function(page, limit, filters) {
                        $location.search('f', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                $scope.createNewOptions = {
                    engines: response.results
                };
            });
        }
    ])
;
