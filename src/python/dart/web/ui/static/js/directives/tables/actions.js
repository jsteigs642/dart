angular
    .module('dart.directives.tables')
    .directive('dtActionsTable', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/tables/actions.html',
            controller: ['$scope', 'ActionService', 'EngineService', '$mdToast', '$window', 'EntityModalService', 'FilterService', '$q', '$mdDialog', '$state',
                function($scope, ActionService, EngineService, $mdToast, $window, EntityModalService, FilterService, $q, $mdDialog, $state) {
                    $scope.$state = $state;
                    $scope.filters = $scope.options.filters || [];
                    $scope.hiddenFilters = $scope.options.hiddenFilters || [];
                    $scope.orderBy = $scope.options.orderBy || [];
                    $scope.$watchCollection("filters", function (newCollection, oldCollection) {
                            if (newCollection.length === 0 && oldCollection.length === 0) { return }
                            $scope.onPaginationChange(1, $scope.query.limit);
                        }
                    );
                    $scope.query = { limit: 10, page: 1 };
                    $scope.onPaginationChange = function(page, limit) {
                        if ($scope.options.onTableChange) {
                            $scope.options.onTableChange(page, limit, $scope.filters)
                        }
                        var filters = $scope.filters.concat($scope.hiddenFilters);
                        $scope.$p = ActionService.getEntities(limit, (page - 1) * limit, filters, $scope.orderBy);
                        $scope.$p.then(function(response) { $scope.response = response });
                        return $scope.$p;
                    };
                    $scope.onPaginationChange(1, $scope.query.limit);
                    $scope.showEntityModal = function(ev, entity, mode) {
                        if (mode === 'delete') {
                            var confirm = $mdDialog.confirm()
                                .title('Are you sure you want to delete this step?')
                                .content('This cannot be undone.')
                                .ariaLabel('delete')
                                .ok('ok')
                                .cancel('cancel')
                                .targetEvent(ev);
                            $mdDialog.show(confirm).then(function() {
                                ActionService.deleteEntity(entity.id)
                            });
                            return;
                        }
                        var e = mode === 'duplicate' ?
                        {
                            data: {
                                name: entity.data.name,
                                action_type_name: entity.data.action_type_name,
                                engine_name: entity.data.engine_name,
                                args: JSON.parse(JSON.stringify(entity.data.args)),
                                order_idx: entity.data.order_idx,
                                state: entity.data.state === 'TEMPLATE' ? 'TEMPLATE' : 'HAS_NEVER_RUN',
                                datastore_id: entity.data.datastore_id,
                                workflow_id: entity.data.workflow_id,
                                on_failure: entity.data.on_failure,
                                on_failure_email: entity.data.on_failure_email,
                                on_success_email: entity.data.on_success_email
                            }
                        } : entity;

                        var saveFunc = null;
                        if (mode === 'edit') {
                            saveFunc = function (entity) { return ActionService.updateEntity(entity).then(function(e) { entity = e; return e }) }
                        }
                        if (mode === 'duplicate') {
                            saveFunc = function (entity) { return ActionService.saveEntity(entity) }
                        }
                        EntityModalService.showDialog(ev, e,
                            function () {
                                return ActionService.getSchema({entity: entity}).then(function(response) {
                                    if (mode === 'view') { response.results.readonly = true; }
                                    return response
                                })
                            },
                            saveFunc
                        )
                    };
                    var engineCallPromise;
                    if ($scope.options.engines) {
                        var dfd = $q.defer();
                        engineCallPromise = dfd.promise;
                        $scope.engines = $scope.options.engines;
                        dfd.resolve()
                    } else {
                        engineCallPromise = EngineService.getEntities(100, 0).then(function(response) { $scope.engines = response.results; });
                    }
                    ActionService.getSchema().then(function(response) {
                        engineCallPromise.then(function() {
                            $scope.engineAndActionTypes = [];
                            $window._.each($scope.engines, function(engine) {
                                $window._.each(engine.data.supported_action_types, function(actionType) {
                                    $scope.engineAndActionTypes.push({
                                        engineAndActionType: {engine: engine, actionType: actionType}
                                    })
                                })
                            });
                            var schemas = [], schema = response.results;
                            $window._.each($scope.engineAndActionTypes, function (obj) {
                                var schemaCopy = JSON.parse(JSON.stringify(schema));
                                var paramsJsonSchema = obj.engineAndActionType.actionType.params_json_schema;
                                schemaCopy.properties.data.properties.args = paramsJsonSchema || {'type': 'null'};
                                schemas.push(schemaCopy)
                            });
                            $scope.getFilterChoices = function(searchText) { return FilterService.getFilterChoices(searchText, schemas) };
                        });
                    });
                }
            ]
        }
    }])
;