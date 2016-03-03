angular
    .module('dart.controllers.graph_load_modal', [])
    .controller('GraphLoadModalController', ['$scope', '$mdDialog', '$q', 'GraphService', 'onSelectedItemChange',
        function($scope, $mdDialog, $q, GraphService, onSelectedItemChange) {
            $scope.onCancel = function() { $mdDialog.hide(); };
            $scope.onSelectedItemChange = function() {
                $mdDialog.hide();
                onSelectedItemChange($scope.selectedItem)
            };
            $scope.querySearch = function(searchText) {
                var deferred = $q.defer();
                GraphService.getGraphIdentifiers(searchText).then(function(response) {
                    deferred.resolve(response.results);
                });
                return deferred.promise;
            };
        }
    ])
;
