
immutable BeliefDict <: Belief
  dict::Dict{String, Vector{Any}}
end

immutable BeliefSQLite <: Belief
  db::SQLite.DB
  table_name::String
end


get_beliefs(b::Belief, sample::String) = error("not implemented")

function get_beliefs(b::BeliefDict, sample::String; thresh=0)
  result = get(b.dict, sample, [])
  if thresh > 0
    result = filter(x->x[2]>thresh, result)
  end
  result
end

function get_beliefs(b::BeliefSQLite, sample::String; thresh=0)
  tbl = b.table_name
  query = "select term_name, prob from $tbl, terms, samples where $(tbl).term_id = terms.term_id " *
          " and $(tbl).sample_id = samples.sample_id and samples.sample_name = ? and prob>?"
  res = SQLite.query(b.db, query, values=[sample, thresh], nullable=false)
  collect(zip(res[:term_name], res[:prob]))
end

get_all_samples(b::Belief) = error("not implemented")
get_all_samples(b::BeliefDict) = keys(b.dict)

function get_all_samples(b::BeliefSQLite)
  tbl = b.table_name
  query = "select distinct sample_name from $tbl, samples where $(tbl).sample_id = samples.sample_id"
  res = SQLite.query(b.db, query, nullable=false)
  Set(res[:sample_name])
end

type BeliefSerializer
    db::SQLite.DB
    sample_dict::Dict{String, Int64}
    term_dict::Dict{String, Int64}
end

function BeliefSerializer(dbfn::String)
    db = SQLite.DB(dbfn)
    SQLite.execute!(db, "PRAGMA journal_mode=OFF;")
    SQLite.execute!(db, "PRAGMA mmap_size=268435456;")
    SQLite.execute!(db, "CREATE TABLE IF NOT EXISTS samples(sample_id INTEGER PRIMARY KEY, sample_name TEXT UNIQUE)")
    SQLite.execute!(db, "CREATE TABLE IF NOT EXISTS terms(term_id INTEGER PRIMARY KEY, term_name TEXT UNIQUE)")
    BeliefSerializer(db, load_dict(db, "samples"), load_dict(db, "terms"))
end

function load_dict(db, tblname)
    rows = SQLite.query(db, "select * from $tblname")
    Dict(zip(rows[:, 2], rows[:, 1]))
end

function get_id(val, db, dict, table_name, colname)
    val = string(val)
    get!(dict, val) do
      SQLite.query(db, "INSERT INTO $(table_name)($colname) VALUES (?)", values=[val])
      id = SQLite.query(db, "select last_insert_rowid()")[1][1]
      id
    end
end

get_sample_id(bs::BeliefSerializer, sample) = get_id(sample, bs.db, bs.sample_dict, "samples", "sample_name")
get_term_id(bs::BeliefSerializer, term) = get_id(term, bs.db, bs.term_dict, "terms", "term_name")


function store_beliefs(bs::BeliefSerializer, belief::BeliefSQLite, tblname; thresh=0.01, overwrite=false)
    if overwrite
      error("not supported")
    end
end
function store_beliefs(bs::BeliefSerializer, belief::BeliefDict, tblname; thresh=0.01, overwrite=true)
    has_table = tblname in SQLite.tables(bs.db)[:name]

    if has_table && !overwrite
      warn("requested no overwrite of $tblname which already exists")
      return
    end

    has_table && overwrite && SQLite.execute!(bs.db, "DROP TABLE $tblname")

    SQLite.execute!(bs.db, "CREATE TABLE $tblname (sample_id INTEGER, term_id INTEGER, prob REAL)")
    stmt = SQLite.Stmt(bs.db,  "INSERT INTO $tblname (sample_id, term_id, prob) VALUES (?, ?, ?)")
    for (sample, values) in belief.dict
        sid = get_sample_id(bs, sample)
        for (term, prob) in values
            prob < thresh && continue

            tid = get_term_id(bs, term)
            SQLite.bind!(stmt, [sid, tid, prob])
            SQLite.execute!(stmt)
        end
    end
    SQLite._close(stmt)
    SQLite.execute!(bs.db, "CREATE INDEX $(tblname)_sample_id_idx on $tblname (sample_id)")
    SQLite.execute!(bs.db, "CREATE VIEW $(tblname)_v(sample, term, prob) as select sample_name as sample, term_name as term, prob " *
                                        "from $(tblname), terms, samples where $(tblname).sample_id=samples.sample_id and " *
                                        "$(tblname).term_id=terms.term_id")
end

function store_params(db, params)
    params_str = JSON.json(params)
    SQLite.execute!(db, "CREATE TABLE IF NOT EXISTS meta(key text, value text)")
    SQLite.query(db, "INSERT INTO meta(key, value) VALUES (?, ?)", values=["params", params_str])
end




function store_beliefs(task::EZLearnTask, serializer::BeliefSerializer)
    gc() # otherwise transaction might fail due to stale statement objects
    SQLite.transaction(serializer.db) do
        store_params(serializer.db, task.params)

        for v in task.views
            id = v.id
            beliefs = task.beliefs[id]
            for (i, belief) in enumerate(beliefs)
                tbl_name = "$(id)_$i"
                println("Storing $(tbl_name)....")
                store_beliefs(serializer, belief, tbl_name)
                beliefs[i] = BeliefSQLite(serializer.db, tbl_name)
                gc()
                println("Done")
            end
        end
    end
end

store_beliefs(task::EZLearnTask, outfn::String) = store_beliefs(task, BeliefSerializer(outfn))

function intersect_labels_core(b1, b2, intersect_fn; keep_left=false, default_left=false, append_left=false, append_right=false)
    shared_samples = intersect(keys(b1), keys(b2))
    result = Dict{String, Vector{String}}()
    for sample in shared_samples
        terms = intersect_fn(b1[sample], b2[sample])
        if keep_left
          terms = union(terms, b1[sample])
        end
        if default_left && length(terms) == 0
          terms = b1[sample]
        end
        if length(terms) > 0
            result[sample] = terms
        end
    end
    if keep_left || default_left || append_left
      for sample in setdiff(keys(b1), keys(b2))
        result[sample] = b1[sample]
      end
    end
    if append_right
      for sample in setdiff(keys(b2), keys(b1))
        result[sample] = b2[sample]
      end
    end
    result
end

intersect_labels_simple(b1, b2) = intersect_labels_core(b1, b2, intersect)

OBOParse.descendants(o::Ontology, terms::Vector{Term}, syms::Vector{Symbol}) = union([descendants(o, t, syms) for t in terms]...)


function ontology_intersector(ontology, symbols)
    function f(vals1, vals2)
        terms1 = Term[gettermbyid(ontology, t) for t in vals1]
        terms2 = Term[gettermbyid(ontology, t) for t in vals2]


        terms1_all = union(terms1, descendants(ontology, terms1, symbols))
        terms2_all = union(terms2, descendants(ontology, terms2, symbols))

        resvals = union(intersect(terms1, terms2_all), intersect(terms2, terms1_all))

        return String[t.id for t in resvals]
    end
    return f
end

function threshold_beliefs{T<:Belief}(beliefs::T, threshold)
    result = Dict{String, Vector{String}}()
    for sample in get_all_samples(beliefs)
        val = get_beliefs(beliefs, sample; thresh=threshold)
        if length(val) > 0
          result[sample] = String[t[1] for t in val]
        end
    end
    result
end


function remove_redundant(ontology, terms::Vector{Term}, rels::Vector{Symbol})
    if length(terms) <= 1
        return terms
    end

    terms = unique(terms)
    termSet = Set(terms)
    n = length(terms)
    keep = ones(Bool, n)
    for i in 1:n
        desc = descendants(ontology, terms[i], rels)
        keep[i] = length(intersect(desc, termSet)) == 0
    end

    terms[keep]
end



getterm(t::Integer, ontology) = gettermbyid(ontology, t)
function getterm(t::String, ontology)
    if haskey(ontology.terms, t)
        return ontology[t]
    else
        return gettermbyid(ontology, parse(Int, t))
    end
end



function threshold_beliefs_nonred(ontology, beliefs, threshold, rels)
    result = Dict{String, Vector{String}}()
    for gsm in get_all_samples(beliefs)
        terms = get_beliefs(beliefs, gsm; thresh=threshold)
        if length(terms) > 0
            #termsids = String[t[1] for t in terms]
            termobjs = Term[getterm(t[1], ontology) for t in terms]
            nonred = remove_redundant(ontology, termobjs, rels)
            @assert length(nonred) > 0
            result[gsm] = String[t.id for t in nonred]
        end
    end
    result
end


function remove_redundant_beliefs(ontology, beliefs, rels)
    result = Dict{String, Vector{String}}()
    for gsm in keys(beliefs)
        terms = beliefs[gsm]
        if length(terms) > 0
            termobjs = Term[gettermbyid(ontology, t) for t in terms]
            nonred = remove_redundant(ontology, termobjs, rels)
            result[gsm] = String[t.id for t in nonred]
        end
    end
    result
end
