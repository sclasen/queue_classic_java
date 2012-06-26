$: << File.expand_path("lib")
$: << File.expand_path("test")

ENV["DATABASE_URL"] ||= "postgresql://localhost:5432/queue_classic_test"

require "queue_classic"
require "minitest/unit"
MiniTest::Unit.autorun

class QCTest < MiniTest::Unit::TestCase

  def setup
    init_db
  end

  def teardown
    QC.delete_all
  end

  def init_db(table_name="queue_classic_jobs")
    QC::Conn.execute("SET client_min_messages TO 'warning'")
    QC::Setup.drop
    QC::Setup.create
    QC::Conn.disconnect
  end

end
