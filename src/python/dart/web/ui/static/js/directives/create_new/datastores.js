angular
    .module('dart.directives.create_new')
    .directive('dtDatastoresCreateNew', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/create_new/datastores.html',
            controller: ['$scope', 'DatastoreService', 'EntityModalService',
                function($scope, DatastoreService, EntityModalService) {
                    $scope.onCreateNew = function(selectedEngine) {
                        if (!selectedEngine) { return; }
                        EntityModalService.showDialog(
                            null,
                            {
                                data: {
                                    name: selectedEngine + '_datastore',
                                    engine_name: selectedEngine
                                }
                            },
                            function () { return DatastoreService.getSchema({engineName: selectedEngine}) },
                            function (entity) { return DatastoreService.saveEntity(entity) }
                        );
                    }
                }
            ]
        }
    }])
;
