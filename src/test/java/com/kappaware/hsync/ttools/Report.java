package com.kappaware.hsync.ttools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * Used to read back generated reports
 * @author sa
 *
 */
@SuppressWarnings("serial")
public class Report extends HashMap<String, Object> {
	private Map<String, Map<String, Object>> nodeByPathByList = new HashMap<String, Map<String, Object>>();

	public List<?> getList(String listName) {
		Object o = this.get(listName);
		if (o != null && o instanceof List<?>) {
			return (List<?>) this.get(listName);
		} else {
			throw new RuntimeException(String.format("Report.%s is not a list!", listName));
		}
	}

	public Object findPathInList(String listName, String path) {
		Map<String, Object> map = this.nodeByPathByList.get(listName);
		if (map == null) {
			map = new HashMap<String, Object>();
			this.nodeByPathByList.put(listName, map);
			List<?> list = this.getList(listName);
			if (list != null) {
				for (Object o : list) {
					if (o instanceof Map) {
						@SuppressWarnings("unchecked")
						String p = (String) ((Map<String, Object>) o).get("path");
						if (p != null) {
							map.put(p, o);
						} else {
							throw new RuntimeException(String.format("No path in object in list %s", listName));
						}
					} else {
						throw new RuntimeException(String.format("Object in list %s is not a map", listName));
					}
				}
			} else {
				throw new RuntimeException(String.format("List %s not found", listName));
			}
		}
		return map.get(path);
	}

}
