angular
    .module('dart.controllers.managers')
    .controller('WorkflowManagerController', ['$scope', 'EngineService', 'WorkflowService', '$q', '$stateParams', '$location', 'DatastoreService', 'TriggerTypeService',
        function($scope, EngineService, WorkflowService, $q, $stateParams, $location, DatastoreService, TriggerTypeService) {
            $scope.initScope = function(selectedItem) {
                $scope.workflowTableOptions = {
                    hideFilters: true,
                    hideControls: true,
                    filters: ['id = ' + selectedItem.id]
                };
                $scope.stepTableOptions = {
                    hiddenFilters: ['workflow_id = ' + selectedItem.id, 'state = TEMPLATE'],
                    filters: JSON.parse($stateParams.sf || '[]'),
                    orderBy: ['order_idx ASC'],
                    onTableChange: function (page, limit, filters) {
                        $location.search('sf', filters.length > 0 ? JSON.stringify(filters) : null);
                    },
                    templateView: true
                };
                DatastoreService.getEntity(selectedItem.data.datastore_id).then(function(response) {
                    var datastore = response.results;
                    EngineService.getEnginesAndActionTypes(datastore.data.engine_name).then(function(results) {
                        $scope.stepCreateNewOptions = {
                            engineAndActionTypes: results.engineAndActionTypes,
                            entityDefaults: { data: { workflow_id: selectedItem.id, state: 'TEMPLATE'}}
                        };
                    });
                });
                $scope.triggerTableOptions = {
                    hiddenFilters: ['workflow_ids IN ' + selectedItem.id],
                    filters: JSON.parse($stateParams.tf || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('tf', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                TriggerTypeService.getEntities(100, 0).then(function(response) {
                    $scope.triggerCreateNewOptions = {
                        triggerTypes: response.results,
                        entityDefaults: { data: { workflow_id: selectedItem.id }}
                    };
                });
                $scope.workflowInstanceTableOptions = {
                    hiddenFilters: ['workflow_id = ' + selectedItem.id],
                    filters: JSON.parse($stateParams.wif || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('wif', filters.length > 0 ? JSON.stringify(filters) : null);
                    }
                };
                $scope.actionTableOptions = {
                    hiddenFilters: ['workflow_id = ' + selectedItem.id],
                    filters: JSON.parse($stateParams.af || '[]'),
                    onTableChange: function (page, limit, filters) {
                        $location.search('af', filters.length > 0 ? JSON.stringify(filters) : null);
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
                WorkflowService.getEntities(20, 0, ['name ~ ' + (searchText || '""')]).then(function(entities) {
                    deferred.resolve(entities.results);
                });
                return deferred.promise;
            };

            if ($stateParams.id) {
                $scope.loading = true;
                WorkflowService.getEntity($stateParams.id).then(function(response) {
                    $scope.selectedItem = response.results;
                    $scope.onSelectedItemChange();
                    $scope.loading = false;
                });
            }

            var tabIndexes = ['wf', 's', 't', 'wfi', 'a'];
            var tabMap = { wf: 0, s: 1, t: 2, wfi: 3, a: 4 };
            $scope.selectedTabIndex = tabMap[$stateParams.t] || 0;
            $scope.$watch('selectedTabIndex', function (newValue) { $location.search('t', tabIndexes[newValue]) });
        }
    ])
;
