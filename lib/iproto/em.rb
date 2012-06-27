require 'eventmachine'
require 'fiber'
module IProto
  module EM
    module FixedHeaderProtocol
      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        def header_size(size = nil)
          if size
            @_header_size = size
          else
            @_header_size
          end
        end
      end

      attr_accessor :header, :body

      def receive_data(data)
        @buffer ||= ''
        offset = 0
        while (chunk = data[offset, _needed_size - @buffer.size]).size > 0 || _needed_size == 0
          @buffer += chunk
          offset += chunk.size
          if @buffer.size == _needed_size
            case _state
            when :receive_header
              @_state = :receive_body
              receive_header @buffer
            when :receive_body
              @_state = :receive_header
              receive_body @buffer
            end
            @buffer = ''
          end
        end 
      end

      def receive_header(header)
        # for override
      end

      def body_size
        # for override
      end

      def receive_body(body)
        # for override
      end

      def _needed_size
        case _state
        when :receive_header
          self.class.header_size
        when :receive_body
          body_size
        end
      end

      def _state
        @_state ||= :receive_header
      end
    end

  class Connection < ::EM::Connection
    include IProto::ConnectionAPI
    include FixedHeaderProtocol

    header_size 12

    def initialize(host, port, reconnect = true)
      @host = host
      @port = port
      @should_reconnect = !!reconnect
      @reconnect_timer = nil
      @waiting_requests = {}
    end

    def connection_completed
      @connected = true
    end

    # begin FixedHeaderAndBody API
    def body_size
      @body_size
    end

    def receive_header(header)
      @type, @body_size, @request_id = header.unpack(PACK)
    end

    def receive_body(data)
      fiber = @waiting_requests.delete @request_id
      raise IProto::UnexpectedResponse.new("For request id #{@request_id}") unless fiber
      fiber.resume data
    end
    # end FixedHeaderAndBody API

    # begin ConnectionAPI
    def send_request(request_type, body)
      request_id = next_request_id
      send_data [request_type, body.size, request_id].pack(PACK) + body
      f = Fiber.current
      @waiting_requests[request_id] = f
      Fiber.yield
    end
    # end

    def close
      close_connection(false)
    end

    def close_connection(*args)
      @should_reconnect = nil
      if @reconnect_timer
        ::EM.cancel_timer @reconnect_timer
        @reconnect_timer = nil
      end
      if @connected
        super(*args)
      end
      discard_requests
    end

    def discard_requests
      exc = IProto::Disconnected.new
      @waiting_requests.keys.each do |req|
        fiber = @waiting_requests.delete req
        fiber.resume exc
      end
    end

    def unbind
      discard_requests
      case @should_reconnect
      when true
        @connected = false
        @reconnect_timer = ::EM.add_timer(0.03) {
          reconnect @host, @port
        }
      when false
        if @connected
          raise IProto::Disconnected
        else
          raise IProto::CouldNotConnect
        end
      when nil
        # do nothing cause we explicitely disconnected
      end
    end
  end

  end
end
