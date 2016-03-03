angular
    .module('dart.directives.create_new')
    .directive('dtActionsCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/actions.html',
            controller: ['$scope', 'ActionService', 'EntityModalService', 'dtUtils',
                function($scope, ActionService, EntityModalService, dtUtils) {
                    $scope.onCreateNew = function(selectedEngineAndActionType) {
                        if (!selectedEngineAndActionType) { return; }
                        EntityModalService.showDialog(
                            null,
                            dtUtils.extend($scope.options.entityDefaults || {}, {
                                data: {
                                    action_type_name: selectedEngineAndActionType.actionType.name,
                                    name: selectedEngineAndActionType.actionType.name,
                                    engine_name: selectedEngineAndActionType.engine.data.name
                                }
                            }),
                            function () {
                                return ActionService.getSchema({
                                    engine_name: selectedEngineAndActionType.engine.data.name,
                                    action_type_name: selectedEngineAndActionType.actionType.name
                                })
                            },
                            function (entity) { return ActionService.saveEntity(entity) }
                        );
                    }
                }
            ]
        }
    }])
;
