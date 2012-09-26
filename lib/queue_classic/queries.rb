module QC
  module Queries
    extend self

    def insert(q_name, method, args, chan=nil)
      QC.log_yield(:action => "insert_job") do
        s = "INSERT INTO #{TABLE_NAME} (q_name, method, args) VALUES (?, ?, ?)"
        Conn.execute(s, q_name, method, OkJson.encode(args))
        Conn.notify(chan) if chan
      end
    end

    def lock_head(q_name, top_bound)
      s = "SELECT * FROM lock_head(?, ?)"
      if r = Conn.execute(s, q_name, top_bound)
        {
          :id     => r["id"],
          :method => r["method"],
          :args   => OkJson.decode(r["args"])
        }
      end
    end

    def count(q_name=nil)
      s = "SELECT COUNT(*) FROM #{TABLE_NAME}"
      s << " WHERE q_name = ?" if q_name
      r = Conn.execute(*[s, q_name].compact)
      r["count"].to_i
    end

    def delete(id)
      Conn.execute("DELETE FROM #{TABLE_NAME} where id = ?::integer", id)
    end

    def delete_all(q_name=nil)
      s = "DELETE FROM #{TABLE_NAME}"
      s << " WHERE q_name = ?" if q_name
      Conn.execute(*[s, q_name].compact)
    end

  end
end
