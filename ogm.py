from abc import ABCMeta
from collections import deque

from py2neo.database import cypher_escape
from py2neo.ogm import Related, OUTGOING, UNDIRECTED, INCOMING
from py2neo.types import remote

class RelatedExtra(metaclass = ABCMeta):

    direction = UNDIRECTED

    def resolve_related_class(self, instance):
        if not isinstance(self.related_class, type):
            module_name, _, class_name = self.related_class.rpartition(".")
            if not module_name:
                module_name = instance.__class__.__module__
            module = __import__(module_name, fromlist=".")
            self.related_class = getattr(module, class_name)

    def __init__(self, related_class, relationship_type=None):
        self.related_class = related_class
        self.relationship_type = relationship_type

    def __get__(self, instance, owner):
        '''
        GraphObject.wrap() calls getattr() on all properties of a GraphObject
        instance. This gives us the ability to store the instance connected
        to the graph.

        This is a hook class, so it returns itself rather than the object(s) it
        refers to.
        '''
        self.source_instance = instance
        self.resolve_related_class(instance)

        return self

    def _relationship_pattern(self, name = 'r', path_length = ''):
        rt = ':' + cypher_escape(self.relationship_type) if self.relationship_type is not None else ''
        if self.direction > 0:
            return '-[{0}{1}{2}]->'.format(name, rt, path_length)
        elif self.direction < 0:
            return '<-[{0}{1}{2}]-'.format(name, rt, path_length)
        else:
            return '-[{0}{1}{2}]-'.format(name, rt, path_length)

RelatedExtra.register(Related)

class SingleRelated(RelatedExtra):
    '''
    Like RelatedTo, this class acts as a hook to another OGM node in the graph.
    From a given OGM instance, you can access another node with a given label,
    related to it by an edge with a given label

    '''

    @property
    def relationship_pattern(self):
        return self._relationship_pattern()

    def __repr__(self):
        return '<SingleRelated: ({0}){1}({2})>'.format(
                self.source_instance.__primarylabel__,
                self.relationship_pattern,
                self.related_class.__primarylabel__
            )

    def __call__(self):
        '''
        Used to access the node to which the hook class refers.

        If the object desired was returned by __get__(), pulling the parent object
        from graph would result in two queries, and we wouldn't be able to use
        __set__ to change the relationship. By calling this hook object,
        it is clearer that the graph is being queried.

        '''
        try:
            # node is stored as a local attribute
            return self.related_node
        except AttributeError:
            self.related_node = self.fetch_node()
            return self.related_node

    def __set__(self, instance, value):
        '''
        Change the relationship
        '''
        assert isinstance(value, self.related_class)
        q = '''
            MATCH (n1) WHERE id(n1) = {{ n1_id }}
            MATCH (n2) WHERE id(n2) = {{ n2_id }}
            OPTIONAL MATCH (n1){0}(n2:{1})
            FOREACH (i IN CASE r WHEN NULL THEN [] ELSE [1] END | DELETE r)
            WITH n1, n2
            CREATE (n1)-[{0}]->(n2)
            RETURN n2
            '''.format(
                self.relationship_pattern,
                self.related_class.__primarylabel__
            )
        e, t = remote(instance.__ogm__.node), remote(value.__ogm__.node)
        d = e.graph.run(q, { 'n1_id': e._id, 'n2_id': t._id }).data()

        self.related_node = value

    def __bool__(self):
        return self() is not None

    def __nonzero__(self):
        return self() is not None

    def __len__(self):
        return 1 if bool(self) else 0

    def fetch_node(self):
        '''
        Explicitly request the node from the graph.
        '''
        e = remote(self.source_instance.__ogm__.node)
        q = '''
            MATCH (n1) WHERE id(n1) = {{ n_id }}
            MATCH (n1){0}(n2:{1})
            RETURN n2
            '''.format(
                self.relationship_pattern,
                self.related_class.__primarylabel__
            )
        d = e.graph.run(q, { 'n_id': e._id }).data()
        assert len(d) <= 1

        if len(d) == 0:
            return None

        return self.related_class.wrap(d[0]['n2'])

class SingleRelatedTo(SingleRelated):
    direction = OUTGOING

class SingleRelatedFrom(SingleRelated):
    direction = INCOMING


class FluentSkipLimit():
    '''
    Fluently add skip and limit features to the query.
    '''

    @property
    def _skip(self):
        try:
            return self.__skip
        except AttributeError:
            return None

    @_skip.setter
    def _skip(self, value):
        self.__skip = value

    def skip(self, skip):
        '''
        fluently set a skip on the number of nodes to retrieve

        :param skip: the starting point in the chain
        '''
        assert isinstance(skip, int)
        self._skip = skip

        # fluent interface
        return self

    def skip_clause(self):
        return 'SKIP {0}'.format(self._skip) if self._skip is not None else ''

    @property
    def _limit(self):
        try:
            return self.__limit
        except AttributeError:
            return None

    @_limit.setter
    def _limit(self, value):
        self.__limit = value

    def limit(self, limit):
        '''
        fluently set a limit on the number of nodes to retrieve

        :param skip: the total number of nodes to return from the starting point
        '''
        assert isinstance(limit, int)
        self._limit = limit

        # fluent interface
        return self

    def limit_clause(self):
        return 'LIMIT {0}'.format(self._limit) if self._limit is not None else ''


class RelatedInChain(RelatedExtra, FluentSkipLimit):
    """
    Represents a sequence of notes about one thing
    """

    @property
    def relationship_pattern(self):
        return self._relationship_pattern(name = '', path_length = '*')

    def __init__(self, relationship_class, relationship_type):
        ''' Iterate over a set of related objects in a chain.

        :param relationship_class: class of object in the chain
        :param relationship_type: edge label connecting the objects
        '''
        super().__init__(relationship_class, relationship_type)


    def __repr__(self):
        return '<RelatedInChain ({0}){1}({2}) skip={3}, limit={4}>'.format(
            self.source_instance.__primarylabel__,
            self.relationship_pattern,
            self.related_class.__primarylabel__,
            self._skip,
            self._limit
        )

    def __len__(self):
        q = '''
        MATCH (s:{0}){1}(t:{2}) WHERE id(s) = {{ s_id }}
        WITH t {3} {4}
        RETURN COUNT(t) as total
        '''.format(
            self.source_instance.__primarylabel__,
            self.relationship_pattern,
            self.related_class.__primarylabel__,
            self.skip_clause(),
            self.limit_clause()
        )
        return graph.run(q, {
            's_id': remote(self.source_instance.__ogm__.node)._id
            }).evaluate()

    def __iter__(self):
        # reset
        try:
            del self.queue
        except AttributeError:
            pass
        return self

    # for python 2...
    def next(self):
        return self.next()

    def __next__(self):
        try:
            return self.related_class.wrap(self.queue.popleft())
        except AttributeError:
            # queue is not populated yet
            # query uses skip/limits rather than path length constraint since
            # this is about 2-3x faster when tested on a 100 node chain.
            self.queue = deque()
            e = remote(self.source_instance.__ogm__.node)
            q = '''
            MATCH (s:{0}){1}(t:{2}) WHERE id(s) = {{ s_id }}
            RETURN t {3} {4}
            '''.format(
                self.source_instance.__primarylabel__,
                self.relationship_pattern,
                self.related_class.__primarylabel__,
                self.skip_clause(),
                self.limit_clause()
            )
            d = e.graph.run(q, {
                's_id': e._id
                }).data()
            if len(d) > 0:
                for r in d:
                    self.queue.append(r['t'])

            return self.related_class.wrap(self.queue.popleft())
        except IndexError:
            # queue is empty, stop the iteration
            pass

        raise StopIteration

class RelatedToInChain(RelatedInChain):
    direction = OUTGOING

class RelatedFromInChain(RelatedInChain):
    direction = INCOMING
