#!/usr/bin/python
import yaml
import StringIO
import pprint
import sys
from logger import *
from misc_funcs import singlewhite

class MsTab():
  def __init__(self, name, key, columns, colmap, descr, props):
    self.name = name
    self.key = key
    self.columns = columns
    self.colmap = colmap
    self.descr = descr
    self.props = props
  def drop_if_exists(self):
    return self.props.get('drop_if_exists',True)

  def __str__(self):
    return str(vars(self))

  def __repr__(self):
    return str(self)

class MsJoin:
  def __init__(self, name, descr, sql, select, aliases, wfilter,props):
    self.name = name
    self.descr = descr
    self.sql = sql
    self.select = select
    self.aliases = aliases
    self.filter = wfilter
    self.props = props

  @staticmethod
  def create(joinentry):
    try:
      sql = joinentry['sql']
    except KeyError, ke:
      sql = None
    if not sql:
      # jtablesmap = reduce(lambda tabs, tab: tabs.__setitem__(tab['name'],tab), joinentry['tables'], {})
      jtablesmap = {}
      for tab in joinentry['tables']:
        jtablesmap[tab['name']] = tab 

      def getalias(tabname_or_alias):
        return jtablesmap[tabname_or_alias]['alias'] if jtablesmap.get(tabname_or_alias) else tabname_or_alias

      def get_tab_from_alias(tab_or_alias):
        for k,v in jtablesmap.iteritems():
          if k == tab_or_alias:
            return k
          elif v['alias'] == tab_or_alias:
            return k
        raise Exception("Unable to find table for alias %s" %tab_or_alias)

      def build_tab_cols(tables_entry):
        def colentry(tabcols, alias, tabcol):
          tabcol=tabcol.strip()
          if '(' in tabcol and ')' in tabcol:
              # return "%s(%s.%s)" % (tabcol[0:tabcol.find('(')],
              #                 alias, tabcol[tabcol.find('(')+1:tabcol.find(')')])
              return tabcol.replace("(","(%s." %alias)
          else:
              return "%s.%s" %(alias,(tabcol if tabcol not in ['ALL','*'] else "*"))
        ret = reduce(lambda tabcols, tabcol: (tabcols+"," if tabcols else '') + colentry(tabcols, tables_entry['alias'], tabcol),
                    tables_entry['columns'].split(','), "")
        return ret

      cols = reduce(lambda allcols, tables_entry: (allcols + ',' if allcols else '') + build_tab_cols(tables_entry),
                    joinentry['tables'], "")
      debug("cols is %s" % cols)
      def tables_from_join_condition(condition):
        tables_or_aliases = map(lambda tabalias : tabalias.strip().split('.')[0], condition.split("="))
        return map(lambda ta_entry : get_tab_from_alias(ta_entry),tables_or_aliases)
      jointabs = reduce(lambda jointabs, jentry:
                        jointabs +
                        ("%s %s"
                          %(tables_from_join_condition(jentry['condition'])[0],
                          getalias(tables_from_join_condition(jentry['condition'])[0]),) if not jointabs else "")
                        + " %s JOIN %s %s on %s "
                        % (jentry['type'],
                           tables_from_join_condition(jentry['condition'])[1],
                           getalias(tables_from_join_condition(jentry['condition'])[1]),
                           jentry['condition']),
                        joinentry['joins'], "")
      # filters = ""
      # for tab in joinentry['tables']:
      #   filters += (" AND " if filters else "") + tab['filters']
      def getfilters(table):
        return reduce(lambda filters, jfilter: filters + (" AND " if filters else "") 
                                       + "%s" %jfilter, table['filters'],"")
      filters = reduce(lambda filters, tab: filters + (" AND " if filters else "") 
                                       + "%s" %getfilters(tab), joinentry['tables'],"")
      if filters:
        filters = " where " + filters
      
      if "grouping_enabled" in joinentry and joinentry["grouping_enabled"]:
        # groupbycols = map(lambda col: col[col.find('(')+1:col.find(')')] if '(' in col else col, cols.split(","))
        groupbycols = filter(lambda col: not '(' in col, cols.split(","))
        groupby = "group by " + reduce(lambda groups, group: (groups + "," if groups else "") + group,groupbycols)

      else:
        groupby = ""
      if 'having' in joinentry:
        having = " having " + reduce(lambda havings, having: havings + (" AND " if havings else "")
                                             + "%s.%s %s\n" %(getalias(having['table']),having['columns'], having['predicate']),joinentry['having'],"")
      else:
        having = ""
      if "orderby" in joinentry:
        orderby = "order by " + reduce(lambda orderbys, orderby: orderbys + ("," if orderbys else "") 
                                             + "%s" %(orderby['columns']),joinentry['orderby'],"")
      else:
        orderby=""
        
      jsql = singlewhite(
        "select %s\nfrom %s %s %s %s %s;" % (cols, jointabs, filters, groupby, having, orderby))
      info("JOIN sql is %s" %jsql)
      return MsJoin(joinentry['name'], joinentry['descr'], jsql, cols, None, None, joinentry)
    else:
      sql = sql.strip()
      if sql[-1] == ';':
        sql = sql[:-1]
      if not " where " in sql:
        sql += " where 1=1"
      sqlre = """select[\s]+(?P<select>[\w., ]+)[\s]+from[\s]+(?P<aliases>[\w.,~= ]+)[\s]+where[\s]+(?P<filter>.*)"""
      import re

      tracerr("about to match %s using %s" % (sql, sqlre))
      groups = re.match(sqlre, sql, re.DOTALL).groupdict()
      return MsJoin(joinentry['name'], joinentry['descr'], sql, groups['select'], groups['aliases'], groups['filter'],joinentry)

  def __str__(self):
    return str(vars(self))

  def __repr__(self):
    return str(self)

class MsMeta():
  def __init__(self, metafile):
    from io_funcs import readfile

    self.yprops = dict(yaml.load(readfile(metafile)))
    self.tmap = self.parse_tables(self.yprops)

  def dump(self):
    pprint.pprint("tables: %s whole shebang: %s" % (self.tmap, self.yprops))

  def table(self, tname):
    return self.tmap[tname]

  def tables(self):
    return self.tmap

  def parse_tables(self, tprops=None):

    tprops = tprops if tprops else self.yprops
    tabprops = tprops['tables']
    print "tabs size is %d type(tabprops[0])=%s tabprops[0]=%s" % (len(tabprops), type(tabprops[0]), tabprops[0])
    tabs = map(lambda t: MsTab(t['name'], t['key'],
                               t['columns'].split('|'), t['columns'].split('|'), t['descr'], t), tabprops)
    for tab in tabs:
      def makecoltype(e):
        return (e.split(':')[0], e.split(':')[1]) if ':' in e else (e, "string")

        # print map(makecoltype, tab.columns)
      colsmap = {}
      colsarr = []
      for name, coltype in map(makecoltype, tab.columns):
        colsmap[name] = coltype
        colsarr.append((name,coltype))

      tab.colmap = colsmap
      tab.columns = colsarr

    tmap = {}
    for tab in tabs:
      tmap[tab.name] = tab
    return tmap

  def joins(self):
    joinvals = self.yprops['joins']
    print "joins are %s" % joinvals
    joins = {}
    for join in map(lambda join: MsJoin.create(join), joinvals):
      joins[join.name] = join
    return joins

  @staticmethod
  def create_meta(meta_file):
    meta = MsMeta(meta_file)
    debug(meta.tmap)
    debug(meta.joins())
    return meta
