angular
    .module('dart.directives.create_new')
    .directive('dtTriggersCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/triggers.html',
            controller: ['$scope', 'TriggerService', 'EntityModalService', 'dtUtils',
                function($scope, TriggerService, EntityModalService, dtUtils) {
                    $scope.onCreateNew = function(selectedTriggerType) {
                        if (!selectedTriggerType) { return; }
                        EntityModalService.showDialog(
                            null,
                            dtUtils.extend($scope.options.entityDefaults || {}, {
                                data: {
                                    trigger_type_name: selectedTriggerType.name,
                                    name: selectedTriggerType.name + '_trigger'
                                }
                            }),
                            function () { return TriggerService.getSchema({trigger_type_name: selectedTriggerType.name}) },
                            function (entity) { return TriggerService.saveEntity(entity) }
                        );
                    }
                }
            ]
        }
    }])
;
