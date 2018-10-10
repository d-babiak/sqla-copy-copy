# CLONED FROM https://groups.google.com/d/msg/sqlalchemy/wb2M_oYkQdY/SsZL8Q1KBAAJ

I'm making some tweaks and putting it up on pip so I don't have to copy/paste

Database is a directed graph

Node is a row

Edge from `a` to `b` if `a` has foreign key referencing `b`

As written, I think it always copies the complete transitive closure of whichever row you start with.

This is more than I want.

Use case:

```
      a
     /
    b
   / \
  c   d
```

If `a has many b's`, I want `deep_copy(b)` to give 

```
        a
      /   \
    b      b'
   / \    / \
  c   d  c'  d'
```

not

```
      a        a'
     /        /
    b        b'
   / \      / \
  c   d    c'  d'
```

I think signature I want is 

```
def sqla_deep_clone(
  entity: Row,
  session: Session,
  not_copied: Set[Table]    # this name sucks - fixed_points? shared?
) -> TransientObjectGraph
```

If a column is part of a foreign key referencing a table that you want to keep fixed (e.g. `a` above

```
def copy_sqla_object(obj, omit_fk=True, not_copied: Set[Table]):
    cls = type(obj)
    mapper = class_mapper(cls)
    newobj = cls()  # not: cls.__new__(cls)
    pk_keys = set([c.key for c in mapper.primary_key])
    rel_keys = set([c.key for c in mapper.relationships])
    prohibited = pk_keys | rel_keys

    if omit_fk:
        fk_keys = set([
          c.key 
          for c in mapper.columns 
          if c.foreign_keys
        ])
        prohibited = prohibited | fk_keys

    log.debug(f"copy_sqla_object: skipping: {prohibited}")

    for k in [
      p.key 
      for p in mapper.iterate_properties
      if (
           (p.key not in prohibited) 
        or (p.key in not_copied)      # the name is really bad
      )
    ]:
        try:
            value = getattr(obj, k)
            log.debug(f"copy_sqla_object: processing attribute {k} = {value}")
            setattr(newobj, k, value)
        except AttributeError:
            log.debug(f"copy_sqla_object: failed attribute {k}")
            pass
    return newobj
```

