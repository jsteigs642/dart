angular
    .module('dart.controllers.entities')
    .controller('WorkflowInstancesEntityController', ['$scope', 'WorkflowInstanceService', 'EntityModalService', '$stateParams', '$location',
        function($scope, WorkflowInstanceService, EntityModalService, $stateParams, $location) {
            $scope.options = {
                filters: JSON.parse($stateParams.f || '[]'),
                onTableChange: function(page, limit, filters) {
                    $location.search('f', filters.length > 0 ? JSON.stringify(filters) : null);
                }
            };
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
        }
    ])
;
