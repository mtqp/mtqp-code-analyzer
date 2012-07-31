
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
