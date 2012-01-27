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

    def connection_completed
      @connected = true
    end

    # begin FixedHeaderAndBody API
    def body_size
      @body_size
    end

    def receive_header(header)
      @type, @body_size, @request_id = header.unpack('L3')
    end

    def receive_body(data)
      fiber = waiting_requests.delete @request_id
      raise IProto::UnexpectedResponse.new("For request id #{@request_id}") unless fiber
      fiber.resume data
    end
    # end FixedHeaderAndBody API

    # begin ConnectionAPI
    def send_request(request_type, body)
      request_id = next_request_id
      send_data [request_type, body.size, request_id].pack('LLL') + body
      f = Fiber.current
      waiting_requests[request_id] = f
      Fiber.yield
    end
    # end

    def waiting_requests
      @waiting_requests ||= {}
    end

    def close_connection(*args)
      super(*args)
    end

    def unbind
      raise IProto::CouldNotConnect.new unless @connected
    end
  end

  end
end