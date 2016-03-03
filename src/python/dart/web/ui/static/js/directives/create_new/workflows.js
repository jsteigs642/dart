angular
    .module('dart.directives.create_new')
    .directive('dtWorkflowsCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/workflows.html',
            controller: ['$scope', 'WorkflowService', 'EntityModalService',
                function($scope, WorkflowService, EntityModalService) {
                    $scope.showNewEntityDialog = function(ev) {
                        EntityModalService.showDialog(
                            ev,
                            $scope.options.entityDefaults || { data: { name: 'workflow' } },
                            function () { return WorkflowService.getSchema() },
                            function (entity) { return WorkflowService.saveEntity(entity) }
                        );
                    };
                }
            ]
        }
    }])
;
