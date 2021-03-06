require File.expand_path("../helper.rb", __FILE__)
require 'rr'

class QueueTest < QCTest
  include RR::Adapters::TestUnit
  extend RR::Adapters::RRMethods

  def test_enqueue
    QC.enqueue("Klass.method")
  end

  def test_lock
    QC.enqueue("Klass.method")
    expected = {:id=>"1", :method=>"Klass.method", :args=>[]}
    assert_equal(expected, QC.lock)
  end

  def test_lock_when_empty
    assert_nil(QC.lock)
  end

  def test_count
    QC.enqueue("Klass.method")
    assert_equal(1, QC.count)
  end

  def test_delete
    QC.enqueue("Klass.method")
    assert_equal(1, QC.count)
    QC.delete(QC.lock[:id])
    assert_equal(0, QC.count)
  end

  def test_delete_all
    QC.enqueue("Klass.method")
    QC.enqueue("Klass.method")
    assert_equal(2, QC.count)
    QC.delete_all
    assert_equal(0, QC.count)
  end

  def test_delete_all_by_queue_name
    p_queue = QC::Queue.new("priority_queue")
    s_queue = QC::Queue.new("secondary_queue")
    p_queue.enqueue("Klass.method")
    s_queue.enqueue("Klass.method")
    assert_equal(1, p_queue.count)
    assert_equal(1, s_queue.count)
    p_queue.delete_all
    assert_equal(0, p_queue.count)
    assert_equal(1, s_queue.count)
  end

  def test_queue_instance
    queue = QC::Queue.new("queue_classic_jobs", false)
    queue.enqueue("Klass.method")
    assert_equal(1, queue.count)
    queue.delete(queue.lock[:id])
    assert_equal(0, queue.count)
  end

  def test_repair_after_error
    queue = QC::Queue.new("queue_classic_jobs", false)
    queue.enqueue("Klass.method")
    assert_equal(1, queue.count)

    times_called = 0
    if RUBY_PLATFORM == "java"
      java_import java.sql.SQLException

      stub(QC::Conn).run_prepared_statement do |statement|
        if times_called == 0
          times_called = times_called + 1
          raise java.sql.SQLException.new("Test exception")
        else
          statement.execute
        end
      end
    else
      connection = QC::Conn.connection
      stub(connection).exec {raise PGError}
    end
    assert_raises(QC::Error) { queue.enqueue("Klass.other_method") }
    assert_equal(1, queue.count)
    queue.enqueue("Klass.other_method")
    assert_equal(2, queue.count)
  rescue QC::Error
    QC::Conn.disconnect
    assert false, "Expected to QC repair after connection error"
  end
end
