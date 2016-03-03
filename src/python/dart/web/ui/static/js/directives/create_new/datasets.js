angular
    .module('dart.directives.create_new')
    .directive('dtDatasetsCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/datasets.html',
            controller: ['$scope', 'DatasetService', 'EntityModalService',
                function($scope, DatasetService, EntityModalService) {
                    $scope.showNewEntityDialog = function(ev) {
                        EntityModalService.showDialog(
                            ev,
                            {},
                            function () { return DatasetService.getSchema() },
                            function (entity) { return DatasetService.saveEntity(entity) }
                        );
                    };
                }
            ]
        }
    }])
;
