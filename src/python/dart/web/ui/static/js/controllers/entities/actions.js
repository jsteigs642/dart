angular
    .module('dart.controllers.entities')
    .controller('ActionsEntityController', ['$scope', 'ActionService', 'EngineService', '$window', 'EntityModalService', '$stateParams', '$location',
        function($scope, ActionService, EngineService, $window, EntityModalService, $stateParams, $location) {
            EngineService.getEnginesAndActionTypes().then(function(results) {
                $scope.tableOptions = {
                    engines: results.engines,
                    filters: JSON.parse($stateParams.f || '[]'),
                    onTableChange: function(page, limit, filters) {
                        $location.search('f', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                $scope.createNewOptions = {
                    engineAndActionTypes: results.engineAndActionTypes
                }
            });
        }
    ])
;