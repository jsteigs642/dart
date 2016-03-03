angular
    .module('dart.controllers.entities')
    .controller('TriggersEntityController', ['$scope', 'TriggerTypeService', '$stateParams', '$location',
        function($scope, TriggerTypeService, $stateParams, $location) {
            TriggerTypeService.getEntities(100, 0).then(function(response) {
                $scope.tableOptions = {
                    triggerTypes: response.results,
                    filters: JSON.parse($stateParams.f || '[]'),
                    onTableChange: function(page, limit, filters) {
                        $location.search('f', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                $scope.createNewOptions = {
                    triggerTypes: response.results
                };
            });
        }
    ])
;
