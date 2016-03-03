angular
    .module('dart.directives.tables')
    .directive('dtTriggersTable', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/tables/triggers.html',
            controller: ['$scope', 'TriggerService', 'EntityModalService', 'FilterService', 'TriggerTypeService', '$window', '$q', '$state',
                function($scope, TriggerService, EntityModalService, FilterService, TriggerTypeService, $window, $q, $state) {
                    $scope.$state = $state;
                    $scope.filters = $scope.options.filters || [];
                    $scope.hiddenFilters = $scope.options.hiddenFilters || [];
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
                        $scope.$p = TriggerService.getEntities(limit, (page - 1) * limit, $scope.filters.concat($scope.hiddenFilters));
                        $scope.$p.then(function(response) { $scope.response = response });
                        return $scope.$p;
                    };
                    $scope.onPaginationChange(1, $scope.query.limit);
                    $scope.showEntityModal = function(ev, entity, mode) {
                        var e = entity;
                        if (mode === 'duplicate') {
                            e = { data: JSON.parse(JSON.stringify(entity.data)) };
                            e.data.state = 'INACTIVE';
                        }
                        var saveFunc = null;
                        if (mode === 'edit') {
                            saveFunc = function (entity) { return TriggerService.updateEntity(entity).then(function(e) { entity = e; return e }) }
                        }
                        if (mode === 'duplicate') {
                            saveFunc = function (entity) { return TriggerService.saveEntity(entity) }
                        }
                        EntityModalService.showDialog(ev, e,
                            function () {
                                return TriggerService.getSchema({entity_id: entity.id}).then(function(response) {
                                    if (mode === 'view') {
                                        response.results.readonly = true;
                                    }
                                    return response
                                })
                            },
                            saveFunc
                        )
                    };
                    var triggerTypeCallPromise;
                    if ($scope.options.triggerTypes) {
                        var dfd = $q.defer();
                        triggerTypeCallPromise = dfd.promise;
                        $scope.triggerTypes = $scope.options.triggerTypes;
                        dfd.resolve()
                    } else {
                        triggerTypeCallPromise = TriggerTypeService.getEntities(100, 0).then(function(response) { $scope.engines = response.results; });
                    }
                    TriggerService.getSchema().then(function(response) {
                        triggerTypeCallPromise.then(function() {
                            var schemas = [], schema = response.results;
                            $window._.each($scope.triggerTypes, function (triggerType) {
                                var schemaCopy = JSON.parse(JSON.stringify(schema));
                                var paramsJsonSchema = triggerType.params_json_schema;
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