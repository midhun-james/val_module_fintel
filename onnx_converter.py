import os
import argparse
import numpy as np

from gliner import GLiNER

import torch
from onnxruntime.quantization import quantize_dynamic, QuantType

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', type=str, default= "logs/model_12000")
    parser.add_argument('--save_path', type=str, default = 'model/')
    parser.add_argument('--quantize', type=bool, default = True)
    args = parser.parse_args()
    
    if not os.path.exists(args.save_path):
        os.makedirs(args.save_path)

    onnx_save_path = os.path.join(args.save_path, "model.onnx")

    print("Loading a model...")
    gliner_model = GLiNER.from_pretrained(args.model_path, load_tokenizer=True)

    labels = ["person", "organization", "location"]
    text = """
    During the annual leadership summit hosted by Zenith Technologies, Dr. Amelia Carter presented her groundbreaking research on sustainable AI systems. As the Chief Innovation Officer at NeuroGrid Solutions, she’s often consulted by major tech giants like Infinisoft Corp and Helix Data Systems. Alongside her was Jonathan Reyes, the Head of Global Strategy at Orion Analytics, whose presentation on "AI in Defense Systems" drew applause from both military and civilian representatives. Their keynote was followed by a Q&A session moderated by Lena Moritz, a journalist from The Future Daily, who has covered tech industry trends for over a decade.
    In attendance was also Rajesh Kumar, CTO of CyberPulse Labs, known for his team's breakthrough work in neural network optimization. Rajesh, who holds security clearance Level 5 under ID CP-9938472A, emphasized the need for ethical frameworks and cross-border regulations. Meanwhile, Emily Sanderson, Project Director at EnviroMind AI, discussed how their systems are now deployed in over 35 countries, aiding in climate change modeling. She provided her contact for follow-ups: emily.s@enviromind.org, and a direct line: +1-415-983-2094.
    Behind the scenes, Carlos D'Souza, an independent security consultant from NetSentinel Group, was seen coordinating with Dr. Fiona MacGregor from BioSys Intelligence to ensure secure real-time data exchange during the live demos. Fiona's employee ID badge, tagged BIS-4428391Z, granted her full access to restricted zones during the showcase. Notably, a brief appearance was made by Sarah-Lee Thomson, the former Head of R&D at NeuraLink Global, whose early research in predictive models laid the foundation for many AI startups today.
    Lunch was sponsored by QuantumEdge Ventures, a VC firm backing companies like NovaSynth AI and PixelForge Interactive. The event was catered by CloudKitchen Co., and the team lead, Michel Tanaka, made sure every guest had a customized meal option. Later in the day, a panel on AI and mental health featured Dr. Benjamin Osei, a psychologist from MindMap Collective, who shared anonymized case studies supported by data IDs like MMH-20230521-1123 for reproducibility in research.
    At the networking gala, Chloe Martinez, known for her work with CivicNet Systems, introduced her recent paper titled “Decentralized Governance and Ethical AI” and exchanged business cards that bore her email: cmartinez@civicnet.com. The evening ended with a fireside chat between Elijah Banks, founder of CodeSight Innovations, and Anita Kapoor, CEO of EcoAI Robotics, discussing the future of AI legislation. Attendees were sent digital summaries via encrypted emails and each delegate’s badge ID, such as DEL-7789210, was logged for security auditing.
    """

    inputs, _ = gliner_model.prepare_model_inputs([text], labels)
        
    if gliner_model.config.span_mode == 'token_level':
        all_inputs =  (inputs['input_ids'], inputs['attention_mask'], 
                        inputs['words_mask'], inputs['text_lengths'])
        input_names = ['input_ids', 'attention_mask', 'words_mask', 'text_lengths']
        dynamic_axes={
            "input_ids": {0: "batch_size", 1: "sequence_length"},
            "attention_mask": {0: "batch_size", 1: "sequence_length"},
            "words_mask": {0: "batch_size", 1: "sequence_length"},
            "text_lengths": {0: "batch_size", 1: "value"},
            "logits": {0: "position", 1: "batch_size", 2: "sequence_length", 3: "num_classes"},
        }
    else:
        all_inputs =  (inputs['input_ids'], inputs['attention_mask'], 
                        inputs['words_mask'], inputs['text_lengths'],
                        inputs['span_idx'], inputs['span_mask'])
        input_names = ['input_ids', 'attention_mask', 'words_mask', 'text_lengths', 'span_idx', 'span_mask']
        dynamic_axes={
            "input_ids": {0: "batch_size", 1: "sequence_length"},
            "attention_mask": {0: "batch_size", 1: "sequence_length"},
            "words_mask": {0: "batch_size", 1: "sequence_length"},
            "text_lengths": {0: "batch_size", 1: "value"},
            "span_idx": {0: "batch_size", 1: "num_spans", 2: "idx"},
            "span_mask": {0: "batch_size", 1: "num_spans"},
            "logits": {0: "batch_size", 1: "sequence_length", 2: "num_spans", 3: "num_classes"},
        }
    print('Converting the model...')
    torch.onnx.export(
        gliner_model.model,
        all_inputs,
        f=onnx_save_path,
        input_names=input_names,
        output_names=["logits"],
        dynamic_axes=dynamic_axes,
        opset_version=14,
    )

    if args.quantize:
        quantized_save_path = os.path.join(args.save_path, "model_quantized.onnx")
        # Quantize the ONNX model
        print("Quantizing the model...")
        quantize_dynamic(
            onnx_save_path,  # Input model
            quantized_save_path,  # Output model
            weight_type=QuantType.QUInt8  # Quantize weights to 8-bit integers
        )
    print("Done!")