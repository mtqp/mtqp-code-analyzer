using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Graph
{
    public class GraphIterator<T>
    {
        private Node<T> _actualNode;
        
        public GraphIterator()
        {
            _actualNode = null;
        }

        public Node<T> ActualNode
        {
            get { return _actualNode; }
        }

        public void SetRoot(Node<T> node)
        {
            _actualNode = node;
        }

        public void StepIntoChild(T child)
        {
            Node<T> childNode = null;
            if (_actualNode != null)
            {
                foreach (Node<T> actualNodeChild in _actualNode.Childs)
                {
                    if (actualNodeChild.Data.Equals(child))
                    {
                        childNode = actualNodeChild;
                        break;
                    }
                }

                if(childNode!=null)
                    _actualNode = childNode;
            }
            else
                throw new Exception("The child to step into does not exist");
        }

        public void RollbackToFather()
        {
            if (_actualNode == null)
                throw new Exception("Iterator has not been set");

            if (_actualNode.Fathers.Count > 0)
                RollbackToFather(_actualNode.Fathers[0]);
            else
                Reset();
        }

        public void RollbackToFather(Node<T> father)
        {
            if (father == null)
                Reset();

            if (_actualNode.IsChildOf(father))
                _actualNode = father;
            else
                throw new Exception("The father to rollback to does not exist");
        }

        public void Reset()
        {
            _actualNode = null;
        }
    }
}
