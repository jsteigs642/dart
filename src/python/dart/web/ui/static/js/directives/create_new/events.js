angular
    .module('dart.directives.create_new')
    .directive('dtEventsCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/events.html',
            controller: ['$scope', 'EventService', 'EntityModalService',
                function($scope, EventService, EntityModalService) {
                    $scope.showNewEntityDialog = function(ev) {
                        EntityModalService.showDialog(
                            ev,
                            { data: { name: 'event' } },
                            function () { return EventService.getSchema() },
                            function (entity) { return EventService.saveEntity(entity) }
                        );
                    };
                }
            ]
        }
    }])
;
