angular
    .module('dart.directives.create_new')
    .directive('dtSubscriptionsCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/subscriptions.html',
            controller: ['$scope', 'SubscriptionService', 'EntityModalService',
                function($scope, SubscriptionService, EntityModalService) {
                    $scope.showNewEntityDialog = function(ev) {
                        EntityModalService.showDialog(
                            ev,
                            { data: { name: 'subscription' } },
                            function () { return SubscriptionService.getSchema() },
                            function (entity) { return SubscriptionService.saveEntity(entity) }
                        );
                    };
                }
            ]
        }
    }])
;
