from dart.model.base import BaseModel, dictable


@dictable
class GraphEntityIdentifier(BaseModel):
    def __init__(self, entity_type=None, entity_id=None, name=None):
        """
        :type entity_type: str
        :type entity_id: str
        :type name: str
        """
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.name = name


class Relationship(object):
    PARENT = 'PARENT'
    CHILD = 'CHILD'


class EntityType(object):
    action = 'action'
    dataset = 'dataset'
    datastore = 'datastore'
    event = 'event'
    subscription = 'subscription'
    trigger = 'trigger'
    workflow = 'workflow'
    workflow_instance = 'workflow_instance'


class Ref(object):
    @staticmethod
    def action(n):
        return Ref.entity(n, EntityType.action)

    @staticmethod
    def dataset(n):
        return Ref.entity(n, EntityType.dataset)

    @staticmethod
    def datastore(n):
        return Ref.entity(n, EntityType.datastore)

    @staticmethod
    def event(n):
        return Ref.entity(n, EntityType.event)

    @staticmethod
    def subscription(n):
        return Ref.entity(n, EntityType.subscription)

    @staticmethod
    def trigger(n):
        return Ref.entity(n, EntityType.trigger)

    @staticmethod
    def workflow(n):
        return Ref.entity(n, EntityType.workflow)

    @staticmethod
    def parent():
        return 'PARENT-1'

    @staticmethod
    def child():
        return 'CHILD-1'

    @staticmethod
    def entity(n, entity_type):
        return 'UNSAVED-%s%s' % (entity_type, n)


@dictable
class SubGraphDefinition(BaseModel):
    def __init__(self, id=None, version_id=None, created=None, updated=None, data=None):
        """
        :type id: str
        :type version_id: int
        :type created: datetime.datetime
        :type updated: datetime.datetime
        :type data: dart.model.graph.SubGraphDefinitionData
        """
        self.id = id
        self.version_id = version_id
        self.created = created
        self.updated = updated
        self.data = data


@dictable
class SubGraphDefinitionData(object):
    def __init__(self, name, description, engine_name=None, related_type=None, related_is_a=None, actions=None,
                 datasets=None, datastores=None, events=None, subscriptions=None, triggers=None, workflows=None,
                 icon=None, md_icon='share'):
        """
        :type name: str
        :type description: str
        :type engine_name: str
        :type related_type: str
        :type related_is_a: str
        :type actions: list[dart.model.action.Action]
        :type datasets: list[dart.model.dataset.Dataset]
        :type datastores: list[dart.model.datastore.Datastore]
        :type events: list[dart.model.event.Event]
        :type subscriptions: list[dart.model.subscription.Subscription]
        :type triggers: list[dart.model.trigger.Trigger]
        :type workflows: list[dart.model.workflow.Workflow]
        :type icon: str
        :type md_icon: str
        """
        self.name = name
        self.description = description
        self.engine_name = engine_name
        self.related_type = related_type
        self.related_is_a = related_is_a
        self.actions = actions
        self.datasets = datasets
        self.datastores = datastores
        self.events = events
        self.subscriptions = subscriptions
        self.triggers = triggers
        self.workflows = workflows
        self.icon = icon
        self.md_icon = md_icon


@dictable
class SubGraph(object):
    def __init__(self, name, description=None, related_type=None, related_is_a=None, graph=None, entity_map=None,
                 icon=None, md_icon=None):
        """
        :type name: str
        :type description: str
        :type related_type: str
        :type related_is_a: str
        :type graph: dart.model.graph.Graph
        :type entity_map: dict[str, dart.model.base.BaseModel]
        :type icon: str
        :type md_icon: str
        """
        self.name = name
        self.description = description
        self.related_type = related_type
        self.related_is_a = related_is_a
        self.graph = graph
        self.entity_map = entity_map
        self.icon = icon
        self.md_icon = md_icon


@dictable
class GraphEntity(BaseModel):
    def __init__(self, entity_type=None, entity_id=None, name=None, state=None, sub_type=None, related_type=None,
                 related_id=None, related_is_a=None):
        """
        :type entity_type: str
        :type entity_id: str
        :type name: str
        :type state: str
        :type sub_type: str
        :type related_type: str
        :type related_id: str
        :type related_is_a: str
        """
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.name = name
        self.state = state
        self.sub_type = sub_type
        self.related_type = related_type
        self.related_id = related_id
        self.related_is_a = related_is_a


@dictable
class Node(BaseModel):
    def __init__(self, entity_type=None, entity_id=None, name=None, state=None, sub_type=None):
        """
        :type entity_type: str
        :type entity_id: str
        :type name: str
        :type state: str
        :type sub_type: str
        """
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.name = name
        self.state = state
        self.sub_type = sub_type


@dictable
class Edge(BaseModel):
    def __init__(self, source_type=None, source_id=None, destination_type=None, destination_id=None):
        """
        :type source_type: str
        :type source_id: str
        :type destination_type: str
        :type destination_id: str
        """
        self.source_type = source_type
        self.source_id = source_id
        self.destination_type = destination_type
        self.destination_id = destination_id


@dictable
class Graph(BaseModel):
    def __init__(self, nodes=None, edges=None):
        """
        :type nodes: list[dart.model.graph.Node]
        :type edges: list[dart.model.graph.Edge]
        """
        self.nodes = nodes
        self.edges = edges
