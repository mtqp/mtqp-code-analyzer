
            foreach(NodeRelation relation in relations)
            {
                List<Line> linesToDraw = GetListOfLinesToDrawRelation(relation);
                foreach (Line line in linesToDraw)
                {
                    if (relation.RelationType == NodeRelationType.DoubleRelated)
                        _pen.Color = COLOR_DOUBLE_RELATION;
                    else
                        _pen.Color = COLOR_SIMPLE_RELATION;
                    _graphics.DrawLine(_pen, line.From, line.To);
                }
            }
        }

        private List<Line> GetListOfLinesToDrawRelation(NodeRelation relation)
        {
            //por ahora no va a busca el camino mas proximo, sin pensar en q se puede pisar 
            //con los objetos ya insertados
            List<Line> lines = new List<Line>();
            Point from = NodeUIState.GetLinePointFrom(relation);
            Point to = NodeUIState.GetLinePointTo(relation);
            Line line = new Line(from, to);
            lines.Add(line);
            return lines;
        }


        private void DrawNode(SpecifierNode node)
        {
            Point topLeft = node.Location;
            Point bottomRight = NodeUIState.BottomRightPoint(node);
            Point innerSpace = NodeUIState.InnerSpace(node);

            string msg = NodeUIState.GetFittedMessage(node);
            Rectangle rectangle = new Rectangle(topLeft.X, topLeft.Y, bottomRight.X, bottomRight.Y);
            _pen.Color = Color.Black;
            _graphics.DrawEllipse(_pen, rectangle);
            _graphics.DrawString(msg, _font, _brush, topLeft.X, topLeft.Y + (innerSpace.Y / 2 - 5));//el texto tendria q poder ponersele enters si llega a ser muy largo...
        }


    }
}
