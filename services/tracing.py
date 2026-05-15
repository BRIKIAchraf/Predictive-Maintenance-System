from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
import os

def setup_tracing(service_name):
    OTLP_ENDPOINT = os.getenv('OTLP_ENDPOINT', 'http://localhost:4317')
    
    resource = Resource(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    
    try:
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True))
        provider.add_span_processor(processor)
    except Exception as e:
        print(f"Tracing disabled: {e}")
        
    trace.set_tracer_provider(provider)
    return trace.get_tracer(service_name)
