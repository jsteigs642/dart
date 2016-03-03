angular
    .module('dart.controllers.managers')
    .controller('DatastoreManagerController', ['$scope', 'EngineService', 'DatastoreService', '$q', '$stateParams', '$location',
        function($scope, EngineService, DatastoreService, $q, $stateParams, $location) {
            $scope.initScope = function(selectedItem) {
                $scope.datastoreTableOptions = {
                    hideFilters: true,
                    hideControls: true,
                    filters: ['id = ' + selectedItem.id]
                };
                $scope.actionTableOptions = {
                    hiddenFilters: ['datastore_id = ' + selectedItem.id],
                    filters: JSON.parse($stateParams.af || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('af', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                EngineService.getEnginesAndActionTypes(selectedItem.data.engine_name).then(function(results) {
                    $scope.actionCreateNewOptions = {
                        engineAndActionTypes: results.engineAndActionTypes,
                        entityDefaults: { data: { datastore_id: selectedItem.id }}
                    };
                });
                $scope.workflowTableOptions = {
                    hiddenFilters: ['datastore_id = ' + selectedItem.id],
                    filters: JSON.parse($stateParams.wf || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('wf', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                $scope.workflowCreateNewOptions = {
                    entityDefaults: { data: { datastore_id: selectedItem.id }}
                };
                $scope.workflowInstanceTableOptions = {
                    hiddenFilters: ['datastore_id = ' + selectedItem.id],
                    filters: JSON.parse($stateParams.wif || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('wif', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
            };

            $scope.onSelectedItemChange = function() {
                if (!$scope.selectedItem) {
                    $location.search('id', null);
                    $location.search('f', null);
                    return
                }
                $location.search('id', $scope.selectedItem.id);
            };

            $scope.querySearch = function(searchText) {
                var deferred = $q.defer();
                DatastoreService.getEntities(20, 0, ['name ~ ' + (searchText || '""')]).then(function(entities) {
                    deferred.resolve(entities.results);
                });
                return deferred.promise;
            };

            if ($stateParams.id) {
                $scope.loading = true;
                DatastoreService.getEntity($stateParams.id).then(function(response) {
                    $scope.selectedItem = response.results;
                    $scope.onSelectedItemChange();
                    $scope.loading = false;
                });
            }

            var tabIndexes = ['d', 'a', 'wf', 'wfi'];
            var tabMap = { d: 0, a: 1, wf: 2, wfi: 3 };
            $scope.selectedTabIndex = tabMap[$stateParams.t] || 0;
            $scope.$watch('selectedTabIndex', function (newValue) { $location.search('t', tabIndexes[newValue]) });
        }
    ])
;
