angular
    .module('dart.directives.tables')
    .directive('dtDatastoresTable', [function() {
        return {
            restrict: 'E',
            scope: {
                options: '='
            },
            templateUrl: 'static/partials/directives/tables/datastores.html',
            controller: ['$scope', 'DatastoreService', 'EngineService', 'EntityModalService', '$window', 'FilterService', '$q', '$state',
                function($scope, DatastoreService, EngineService, EntityModalService, $window, FilterService, $q, $state) {
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
                        $scope.$p = DatastoreService.getEntities(limit, (page - 1) * limit, $scope.filters.concat($scope.hiddenFilters));
                        $scope.$p.then(function(response) { $scope.response = response });
                        return $scope.$p;
                    };
                    $scope.onPaginationChange(1, $scope.query.limit);
                    $scope.showEntityModal = function(ev, entity, mode) {
                        var e = entity;
                        if (mode === 'duplicate') {
                            e = { data: JSON.parse(JSON.stringify(entity.data)) };
                            e.data.state = 'INACTIVE';
                            e.data.host = null;
                            e.data.port = null;
                            e.data.connection_url = null;
                            e.data.s3_artifacts_path = null;
                            e.data.s3_logs_path = null;
                        }
                        var saveFunc = null;
                        if (mode === 'edit') {
                            saveFunc = function (entity) { return DatastoreService.updateEntity(entity).then(function(e) { entity = e; return e }) }
                        }
                        if (mode === 'duplicate') {
                            saveFunc = function (entity) { return DatastoreService.saveEntity(entity) }
                        }
                        EntityModalService.showDialog(ev, e,
                            function () {
                                return DatastoreService.getSchema({engineName: entity.data.engine_name}).then(function(response) {
                                    if (mode === 'view') {
                                        response.results.readonly = true;
                                    }
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
                    DatastoreService.getSchema().then(function(response) {
                        engineCallPromise.then(function() {
                            var schemas = [], schema = response.results;
                            $window._.each($scope.engines, function (engine) {
                                var schemaCopy = JSON.parse(JSON.stringify(schema));
                                var optionsJsonSchema = engine.data.options_json_schema;
                                schemaCopy.properties.data.properties.args = optionsJsonSchema || {'type': 'null'};
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