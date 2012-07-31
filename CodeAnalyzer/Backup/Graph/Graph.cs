using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Graph
{
    public class Graph<T> where T : ISortable
    {
        private List<Node<T>> _nodes;
        private GraphIterator<T> _iterator;

        public Graph()
        {
            _nodes = new List<Node<T>>();
            _iterator = new GraphIterator<T>();
        }

        public int Count
        {
            get { return _nodes.Count; }
        }

        public GraphIterator<T> Iterator
        {
            get { return _iterator; }
        }

        public void AddNode(T data)
        {
            Node<T> node = new Node<T>(data);

            if (!_nodes.Contains(node))
            {
                _nodes.Add(node);

                if (_nodes.Count == 1 || _iterator.ActualNode == null)
                    _iterator.SetRoot(node);
            }
            else
                throw new Exception("The node already exists");
        }

        public void Relate(T father, T child)
        {
            Node<T> fatherNode = null;
            Node<T> childNode = null;

            foreach (Node<T> node in _nodes)
            {
                if (node.Data.Equals(father))
                    fatherNode = node;
                else if (node.Data.Equals(child))
                    childNode = node;
            }
            if (fatherNode == null || childNode == null)
                throw new Exception("Cannot relate nodes that are not in the graph");

            fatherNode.AddChild(childNode);
            childNode.AddFather(fatherNode);
        }

        public List<T> GetNodesByDFS()
        { 
            List<T> data = GetData();
            return Sort.IdiotSort<T>(data);
        }

        private List<T> GetData()
        {
            List<T> dataList = new List<T>();

            foreach (Node<T> node in _nodes)
                dataList.Add(node.Data);

            return dataList;
        }

        //public void RemoveNode(Node<T> node)
        //{
        //    if (_nodes.Contains(node))
        //    {
        //        if (_iterator.ActualNode == node || node.Fathers.Count > 0)
        //            _iterator.RollbackToFather(node.Fathers[0]);
        //        else
        //            _iterator.Reset();

        //        RemoveRelations(node);
        //        _nodes.Remove(node);
        //    }
        //    else
        //        throw new Exception("The node to delete doesn´t exist in the graph");
        //}

        //public void Unrelate(Node<T> father, Node<T> child)
        //{
        //    if (!_nodes.Contains(father) || !_nodes.Contains(child))
        //        throw new Exception("Cannot unrelate nodes that are not in the graph");

        //    if (father.IsFatherOf(child))
        //    {
        //        father.RemoveChild(child);
        //        child.RemoveFather(father);
        //    }
        //}

        ////ojo que esto no se como anda con el aliasing
        //private void RemoveRelations(Node<T> node)
        //{
        //    foreach (Node<T> father in node.Fathers)
        //    {
        //        //Node<T> graphFather = GetNode(father);
        //        father.RemoveChild(node);
        //    }

        //    foreach (Node<T> child in node.Childs)
        //    {
        //        //Node<T> graphChild = GetNode(child);
        //        child.RemoveFather(node);
        //    }
        //}

        //private Node<T> GetNode(Node<T> node)
        //{
        //    foreach (Node<T> graphNode in _nodes)
        //        if (graphNode == node)
        //            return graphNode;
        //    throw new Exception("The node doesn´t exist");
        //}
        
    }
}
