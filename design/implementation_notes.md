# Data links

* Currently links to deleted objects can be returned from `get_links_from_data`.
  * Should this be changed? It means an extra workspace call.
  * It also has reproducibility issues - calls to the method with the same `effective_time` may
    not produce the same results.
    * That being said, changing permissions to workspaces can also change what links are returned
      over time.
  * If we don't return links to deleted objects, should the links be autoexpired if they aren't
    already?
    * This assumes `get_object_info3` with `ignoreErrors: 1` will only return `null` for deleted
      objects when called via `administer` - verify
  * What about expired links to deleted objects with an `effective_time` in the past? Return them?

* Links to deleted objects can be expired as long as the user has write access to the workspace.
  However, links to objects in deleted workspaces **cannot** be expired by anyone, including
  admins, given the current implementation.
  * This seems ok since the links aren't accessible by anyone other than admins.