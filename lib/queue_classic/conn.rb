require 'jdbc/postgres'
require 'cgi'

module QC
  module Conn
    extend self

    def run_prepared_statement(statement)
      statement.execute
    end

    def execute(stmt, *params)
      pstment = connection.prepareStatement(stmt)
      params.each_with_index do |p,i|
        if p.is_a? Fixnum
          pstment.setInt(i+1, p)
        else
          pstment.setString(i+1, p)
        end
      end

      log(:level => :debug, :action => "exec_sql", :sql => stmt.inspect)
      begin
        if stmt =~ /^SELECT/
          pg_results = pstment.executeQuery
          meta_data = pg_results.getMetaData()
          rows = []
          while pg_results.next
            row = {}
            (1..meta_data.column_count).each do |i|
              row[meta_data.getColumnName(i)] = pg_results.get_string(i)
            end
            rows << row
          end
          rows.length > 1 ? rows : rows.pop
        else
          run_prepared_statement(pstment)
        end
      rescue java.sql.SQLException => e
        log(:error => e.inspect)
        disconnect
        raise QC::Error, e.message
      end
    end

    def notify(chan)
      log(:level => :debug, :action => "NOTIFY")
      execute('NOTIFY "' + chan + '"') #quotes matter
    end

    def listen(chan)
      log(:level => :debug, :action => "LISTEN")
      execute('LISTEN "' + chan + '"') #quotes matter
    end

    def unlisten(chan)
      log(:level => :debug, :action => "UNLISTEN")
      execute('UNLISTEN "' + chan + '"') #quotes matter
    end

    def drain_notify
      until connection.notifies.nil?
        log(:level => :debug, :action => "drain_notifications")
      end
    end

    def wait_for_notify(t)
      connection.wait_for_notify(t) do |event, pid, msg|
        log(:level => :debug, :action => "received_notification")
      end
    end

    def transaction
      begin
        execute("BEGIN")
        yield
        execute("COMMIT")
      rescue Exception
        execute("ROLLBACK")
        raise
      end
    end

    def transaction_idle?
      connection.transaction_status == PGconn::PQTRANS_IDLE
    end

    def connection
      @connection ||= connect
    end

    def disconnect
      connection.close
    ensure
      @connection = nil
    end

    def connect
      url_params = CGI::parse(db_url.query || "")
      props = java.util.Properties.new
      props.setProperty("user", url_params["user"].empty? ? ENV["USER"] : url_params["user"].first)
      props.setProperty("password", url_params["password"].empty? ? "" : url_params["password"].first)
      port_str = db_url.port ? ":" + db_url.port.to_s : ""
      conn = Java::OrgPostgresql::Driver.new.connect("jdbc:" + db_url.scheme + "://" + db_url.host + port_str + db_url.path, props)
      log(:level => :debug, :action => "establish_conn")
      if conn.is_closed
        log(:level => :error, :message => conn.error)
      end
      conn
    end

    def db_url
      return @db_url if @db_url
      url = env_db_url
      @db_url = URI.parse(url)
    end

    def env_db_url
      ENV["QC_DATABASE_URL"] ||
      ENV["DATABASE_URL"]    ||
      raise(ArgumentError, "missing QC_DATABASE_URL or DATABASE_URL")
    end

    def log(msg)
      QC.log(msg)
    end

  end
end
